from datetime import datetime
import importlib
from io import StringIO
from pathlib import Path
import re
import traceback
from typing import Any, Callable, Dict, List, Union
import sys

# Prism-specific imports
from prism.client.parser import ProjectParser
from prism.callbacks.callback import _PrismCallback
from prism.connectors.base import Connector
from prism.db.mixins import DbMixin
from prism.engine.executor import _DagExecutor
from prism.engine.module import _PrismModule
import prism.logging.execution
from prism.logging.events import (
    fire_header_events,
    fire_section_event,
    fire_callback_events,
    fire_empty_line_event,
    fire_tail_events,
)
import prism.logging.loggers


class ProjectRunner(DbMixin):
    project_dir: Path
    project_id: str
    rich_logging: bool
    log_file: Union[str, Path, StringIO]
    run_id: str
    run_slug: str
    tasks_dir: Union[str, Path]
    task_ids: List[str]
    runtime_ctx: Dict[str, Any]
    connectors: List[Connector]
    concurrency: int
    package_lookups: List[Union[Path, str]]
    all_tasks_upstream: bool
    all_tasks_downstream: bool
    on_success: List[Union[str, Callable[[], Any]]]
    on_failure: List[Union[str, Callable[[], Any]]]
    full_refresh: bool

    on_success_callbacks: List[_PrismCallback]
    on_failure_callbacks: List[_PrismCallback]
    project_parser: ProjectParser

    def __init__(
        self,
        project_dir: Path,
        project_id: str,
        rich_logging: bool,
        log_file: Union[str, Path, StringIO],
        run_id: str,
        run_slug: str,
        tasks_dir: Union[str, Path],
        task_ids: List[str],
        runtime_ctx: Dict[str, Any],
        connectors: List[Union[str, Connector]],
        concurrency: int,
        package_lookups: List[Union[Path, str]],
        all_tasks_upstream: bool,
        all_tasks_downstream: bool,
        on_success: List[Union[str, Callable[[], Any]]],
        on_failure: List[Union[str, Callable[[], Any]]],
        full_refresh: bool,
    ):
        self.project_dir = project_dir
        self.project_id = project_id
        self.rich_logging = rich_logging
        self.run_id = run_id
        self.run_slug = run_slug
        self.tasks_dir = tasks_dir
        self.task_ids = task_ids
        self.runtime_ctx = runtime_ctx
        self.concurrency = concurrency
        self.package_lookups = package_lookups
        self.all_tasks_upstream = all_tasks_upstream
        self.all_tasks_downstream = all_tasks_downstream
        self.on_success = on_success
        self.on_failure = on_failure
        self.full_refresh = full_refresh

        # Update database
        if isinstance(log_file, StringIO):
            log_file = ""
        super().create_new_run(
            run_slug=run_slug,
            run_date=datetime.now(),
            logs_path=str(log_file),
            status="PENDING",
            ctx=self.runtime_ctx,
            project_id=self.project_id,
        )

        # Make sure all the package lookups have been added to our sys.path
        for path in self.package_lookups:
            if path not in sys.path:
                sys.path.insert(0, str(path))

        # Create callback objects
        self.on_success_callbacks = []
        self.on_failure_callbacks = []
        for scb in self.on_success:
            if isinstance(scb, str):
                self.on_success_callbacks.append(_PrismCallback.from_str(scb))
            else:
                self.on_success_callbacks.append(_PrismCallback(scb))
        for fcb in self.on_failure:
            if isinstance(fcb, str):
                self.on_success_callbacks.append(_PrismCallback.from_str(fcb))
            else:
                self.on_failure_callbacks.append(_PrismCallback(fcb))

        # Create connector objects
        self.connectors = []
        for cn in connectors:
            if isinstance(cn, str):
                module_name = ".".join(cn.split(".")[:-1])
                connector_name = cn.split(".")[-1]
                mod = importlib.import_module(module_name)
                if not hasattr(mod, connector_name):
                    raise RuntimeError(
                        f"Could not find connector `{connector_name}` in module `{module_name}`"  # noqa: E501
                    )
                conn = getattr(mod, connector_name)
                assert isinstance(conn, Connector)
                self.connectors.append(conn)
            else:
                self.connectors.append(cn)

        # Project parser
        self.project_parser = ProjectParser(
            self.project_dir, self.tasks_dir, self.all_tasks_downstream
        )

        # Create CurrentRun object. This takes advantage of Python's import caching.
        runtime = importlib.import_module("prism.runtime")
        # TODO: clean up this error
        if not hasattr(runtime, "CurrentRun"):
            raise ValueError("runtime does not have `CurrentRun` attribute!")
        runtime.CurrentRun._setup(
            run_id=self.run_id, runtime_ctx=self.runtime_ctx, connectors=self.connectors
        )

    def _parse_all_modules(
        self,
        project_dir: Path,
        root_task_dir: Union[str, Path],
        module_relpaths: List[str],
    ) -> List[_PrismModule]:
        """
        Create a `_PrismModule` object for each module in `module_relpaths`. This object
        contains all the task nodes in the module (along with the task's refs and
        targets).

        args:
            project_dir: project directory
            root_task_dir: root directory for tasks
            module_relpaths: list of relative paths for modules inside `root_task_dir`
        returns:
            list of _PrismModule objects
        """
        prism_modules: List[_PrismModule] = []
        for relpath in module_relpaths:
            mod = _PrismModule(project_dir, root_task_dir, relpath)
            if len(mod.prism_task_nodes.keys()) > 0:
                prism_modules.append(mod)
        return prism_modules

    def run(self):
        # Wrap everything in a try-except block so that we don't have to create a bunch
        # of smaller try-except blocks for each step.
        super().update_run_status(self.run_slug, self.project_id, "RUNNING")

        # TODO: make sure exception formatting is solid
        try:
            # Total number of tasks / targets
            parsed_module_objs = self.project_parser.parse_all_modules()
            num_modules = len(parsed_module_objs)
            num_tasks = 0
            for mod in parsed_module_objs:
                num_tasks += len(mod.prism_task_nodes.keys())
            fire_header_events(
                project_id=self.project_id,
                run_slug=self.run_slug,
                num_tasks=num_tasks,
                num_modules=num_modules,
            )

            # If the task IDs are in the format `directory/*`, then parse all nested
            # directories and grab the task IDs as a list.
            dir_task_ids = []
            for idx, tid in enumerate(self.task_ids):
                if re.findall(r"^.+\/\*$", tid):
                    self.task_ids.pop(idx - 1)
                    directory = "/".join(tid.split("/")[:-1])
                    dir_task_ids.append(directory)
            for _m in parsed_module_objs:
                if any([x in _m.module_task_relpath for x in dir_task_ids]):
                    self.task_ids.extend(_m.prism_task_ids)

            # Compile the DAG
            compiled_dag_em = prism.logging.execution._ExecutionEventManager(
                idx=None,
                total=None,
                name="Parsing task dependencies",
                func=self.project_parser.compile_dag,
            )
            compiled_dag = compiled_dag_em.run(
                fire_exec_events=True,
                project_id=self.project_id,
                run_slug=self.run_slug,
                tasks_dir=self.tasks_dir,
                parsed_module_objs=parsed_module_objs,
                user_arg_task_ids=self.task_ids,
                user_arg_all_downstream=self.all_tasks_downstream,
            )
            super().update_project_tasks_refs_targets(
                project_id=self.project_id,
                tasks=compiled_dag.all_task_ids,
                refs=compiled_dag.task_refs,
                targets=compiled_dag.task_targets,
            )

            # Execute the tasks in their topological sort
            fire_section_event(section_title="Tasks")
            dag_executor = _DagExecutor(
                compiled_dag=compiled_dag,
                user_arg_all_upstream=self.all_tasks_upstream,
                user_arg_all_downstream=self.all_tasks_downstream,
                threads=self.concurrency,
                full_refresh=self.full_refresh,
            )
            dag_executor.exec()
            super().update_run_status(self.run_slug, self.project_id, "SUCCEEDED")

            # Callbacks
            if self.on_success_callbacks:
                fire_section_event(section_title="Callbacks")
                fire_callback_events(self.run_slug, "on_success")
                fire_empty_line_event()

                # fire_section_event()
                for scb in self.on_success_callbacks:
                    scb_em = prism.logging.execution._ExecutionEventManager(
                        idx=None,
                        total=None,
                        name=f"Running [blue]{scb.name}[/blue] callback",
                        func=scb.run,
                    )
                    scb_em.run(fire_exec_events=True)

            # Remove package lookups from our sys.path
            for path in self.package_lookups:
                try:
                    sys.path.remove(str(path))
                except ValueError:
                    pass

            # In addition, remove all the task modules that were imported
            mods_to_del: List[str] = []
            for sysmod, obj in sys.modules.items():
                try:
                    if obj.__file__:
                        if (
                            str(self.project_dir) in str(obj.__file__)
                            or str(self.tasks_dir) in str(obj.__file__)  # noqa: W503
                            or any(  # noqa: W503
                                [
                                    str(x) in str(obj.__file__)
                                    for x in self.package_lookups
                                ]
                            )  # noqa: E501, W503
                        ):
                            mods_to_del.append(sysmod)

                    # Namespace loaders have a `None` __file__ attribute. Their path
                    # information is stored in the `__path__` attribute
                    else:
                        namespace_paths = obj.__path__
                        for np in namespace_paths:
                            if (
                                str(self.project_dir) in np
                                or str(self.tasks_dir) in np  # noqa: W503
                                or any(  # noqa: W503
                                    [str(x) in np for x in self.package_lookups]
                                )  # noqa: E501, W503
                            ):
                                mods_to_del.append(sysmod)
                except AttributeError:
                    pass

            for dmod in mods_to_del:
                del sys.modules[str(dmod)]

            fire_tail_events()
            return None
        except Exception:
            super().update_run_status(self.run_slug, self.project_id, "FAILED")

            if self.on_failure_callbacks:
                fire_section_event(section_title="Callbacks")
                fire_callback_events(self.run_slug, "on_failure")
                fire_empty_line_event()
                for fcb in self.on_failure_callbacks:
                    fcb_em = prism.logging.execution._ExecutionEventManager(
                        idx=None,
                        total=None,
                        name=f"Running [blue]{fcb.name}[/blue] callback",
                        func=fcb.run,
                    )
                    fcb_em.run(fire_exec_events=True)

            # Remove package lookups from our sys.path
            for path in self.package_lookups:
                try:
                    sys.path.remove(str(path))
                except ValueError:
                    pass

            # In addition, remove all the task modules that were imported
            mods_to_del = []
            for sysmod, obj in sys.modules.items():
                try:
                    if obj.__file__:
                        if (
                            str(self.project_dir) in obj.__file__
                            or str(self.tasks_dir) in obj.__file__  # noqa: W503
                            or any(  # noqa: W503
                                [str(x) in obj.__file__ for x in self.package_lookups]
                            )  # noqa: E501, W503
                        ):
                            mods_to_del.append(sysmod)
                except AttributeError:
                    pass
                except TypeError:
                    pass
            for dmod in mods_to_del:
                del sys.modules[dmod]

            # Silently log the traceback so that it appears in our file
            exc_type, exc_value, tb = sys.exc_info()
            tb_stack = traceback.format_exception(exc_type, exc_value, tb, limit=None)
            prism.logging.loggers.console_print(tb_stack)

            if self.rich_logging:
                prism.logging.loggers.CONSOLE.print_exception(
                    show_locals=False, width=120
                )
                sys.exit(1)
            else:
                raise
