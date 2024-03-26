from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Union

# Prism-specific imports
from prism.admin import generate_run_id, generate_run_slug
from prism.client.runner import ProjectRunner
from prism.client.visualizer import PrismVisualizer
from prism.connectors.base import Connector
from prism.constants import INTERNAL_FOLDER
from prism.db.mixins import DbMixin
from prism.logging import set_up_logger


class PrismProject(DbMixin):
    id: str
    name: str
    version: str
    connectors: List[Union[str, Connector]]
    concurrency: int
    tasks_dir: Union[str, Path]
    package_lookups: List[Union[str, Path]]
    on_success: List[Union[str, Callable[[], Any]]]
    on_failure: List[Union[str, Callable[[], Any]]]
    ctx: Dict[str, Any]
    project_dir: Path

    def __init__(
        self,
        id: str = "",
        name: str = "",
        version: str = "",
        connectors: Optional[List[Union[str, Connector]]] = None,
        concurrency: int = 1,
        tasks_dir: Union[str, Path] = Path.cwd() / "tasks",
        package_lookups: Optional[List[Union[Path, str]]] = None,
        on_success: Optional[List[Union[str, Callable[[], Any]]]] = None,
        on_failure: Optional[List[Union[str, Callable[[], Any]]]] = None,
        ctx: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize your Prism project.

        args:
        - `id`: unique ID to give this project. If not specified, then Prism will create
            an ID for you. This ID will be in the format `{project dir}-{version}`
        - `name`: human-readable name to give this project. If not specified, then all
            Prism will create a name for you. This name will be the name of the project
            directory.
        - `version`: project version. Defaults to 1.0.
        - `connectors`: list of connectors to use in your project. These can be accessed
            at runtime `CurrentRun.conn(...)`. Using connectors prevents you from having
            to define your connection class in each module. Connectors should either be
            specified as a Connector object or as a string representing the import path
            to the Connector object.
        - `concurrency`: number of threads to use when running tasks. Default is `1`
            (i.e., single-threaded)
        - `tasks_dir`: directory containing tasks. Default is the "tasks" folder in the
            current directory.
        - `package_lookups`: additional directories / modules to look within when
            importing modules and functions in your code. The `tasks_dir` and its parent
            are automatically added to this list.
        - `on_success`: list of callbacks to run when the run succeeds. Callbacks should
            be functions that do not accept any argument. Callbacks should either be
            specified as a function or as a string representing the import path to the
            function.
        - `on_failure`: list of callbacks to run when the run fails. Callbacks should
            be functions that do not accept any argument. Callbacks should either be
            specified as a function or as a string representing the import path to the
            function.

        Example usage:

        ```python
        from pathlib import Path
        from prism.client import PrismProject


        def print_success():
            print("Success!")


        project = PrismProject(
            tasks_dir=Path.cwd() / "tasks",  # default task directory
            package_lookups="/Users/common",  # outside our working project dir
            on_success=[print_success],  # print `Success!` when the project succeeds
        )

        if __name__ == "__main__":
            project.run()
        ```
        """
        # Project path
        self.project_dir = Path.cwd()

        # Metadata
        self.name = name if name != "" else f"{self.project_dir.name.lower()}"
        self.version = version if version != "" else "1.0"
        self.id = id if id != "" else f"{self.project_dir.name.lower()}-{self.version}"
        self.connectors = [] if connectors is None else connectors
        self.concurrency = concurrency
        self.tasks_dir = tasks_dir
        self.package_lookups = [] if package_lookups is None else package_lookups
        self.on_success = [] if on_success is None else on_success
        self.on_failure = [] if on_failure is None else on_failure
        self.ctx = {} if ctx is None else ctx

        super().create_new_project(self.id, self.project_dir, self.ctx)

    def graph(
        self,
        port: int = 8000,
        open_window: bool = True,
        hot_reload: bool = True,
        log_level: Literal["info", "warning", "error", "debug", "critical"] = "info",
    ):
        set_up_logger(log_level, None)
        visualizer = PrismVisualizer(
            project_id=self.id,
            project_dir=self.project_dir,
            tasks_dir=self.tasks_dir,
            port=port,
            open_window=open_window,
            hot_reload=hot_reload,
        )
        visualizer.graph()

    def run(
        self,
        run_id: Optional[str] = None,
        task_ids: Optional[List[str]] = None,
        runtime_ctx: Optional[Dict[str, Any]] = None,
        all_tasks_upstream: bool = False,
        all_tasks_downstream: bool = False,
        on_success: Optional[List[Union[str, Callable[[], Any]]]] = None,
        on_failure: Optional[List[Union[str, Callable[[], Any]]]] = None,
        full_refresh: bool = False,
        log_level: Literal["info", "warning", "error", "debug", "critical"] = "info",
        rich_logging: bool = True,
        log_file: Optional[Union[str, Path, StringIO]] = None,
    ) -> None:
        """
        Run a Prism project with fine-grained, granular control. For example, you can
        use the `task_ids` keyword argument to specify which tasks are run. Or, you can
        use the `runtime_ctx` keyword argument to overwrite variables in the
        PrismProject's `ctx`.

        args:
        - `run_id`: unique ID to give this run. If not specified, then Prism will create
            an ID for you. This run ID will be in the format `{project ID}-{uuid}`
        - `task_ids`: list of task IDs to run. If not specified, then all tasks
            are run. Tasks are retrieved from the `tasks_dir` path specified in the
            PrismProject's instantiation.
        - `runtime_ctx`: variables to add to PrismProject's `ctx`. Note that variables
            defined in the`runtime_ctx` will *overwrite* like-named variables
            defined in the PrismProject's `ctx`.
        - `all_tasks_upstream`: boolean controlling whether to run all tasks
            upstream of those specified in `task_ids`. Default is `False`.
        - `all_tasks_downstream`: boolean controlling whether to run all tasks
            downstream of those specified in `task_ids`. Default is `True`.
        - `on_success`: list of callbacks to run when the run succeeds. These are run
            in addition to the callbacks specified in the project's instantiation.
        - `on_failure`: list of callbacks to run when the run fails. These are run
            in addition to the callbacks specified in the project's instantiation.
        - `full_refresh`: run all the tasks, regardless of whether or not they are
            already `done`.
        - `log_level`: logging level, one of `info`, `warn`, `error`, `debug`, or
            `critical`.
        - `rich_logging`: beautify logs in the console with the `rich` package. Default
            is `True`.
        - `log_file`: file in which to save the logs. If `None`, then Prism will default
            to a file within `~/.prism/logs/`.

        returns:
        None
        """
        # Default args for mutable types
        task_ids = task_ids if task_ids is not None else []
        runtime_ctx = runtime_ctx if runtime_ctx is not None else {}
        on_success = on_success if on_success else []
        on_failure = on_failure if on_failure else []

        # Run ID and run slug
        run_id = run_id if run_id is not None else f"{self.id}-{generate_run_id()}"
        run_slug = generate_run_slug()

        # If the log file is not specified, then create a log file
        tmp_date = datetime.now().strftime("%Y%m%d%H%M%S")
        if log_file is None:
            log_file = (
                Path(INTERNAL_FOLDER).resolve() / "logs" / f"{tmp_date}_{run_slug}.log"
            )  # noqa: E501
        if isinstance(log_file, Path):
            Path(log_file).parent.mkdir(exist_ok=True)
        set_up_logger(log_level, log_file, rich_logging)
        self.ctx.update(runtime_ctx)

        # Add the tasks directory (and its parent) to the `package_lookups` attribute
        for lookup_path in [
            self.project_dir,
            Path(self.tasks_dir),
            Path(self.tasks_dir).parent,
        ]:
            if str(lookup_path) not in self.package_lookups:
                self.package_lookups.append(str(lookup_path))

        runner = ProjectRunner(
            project_dir=self.project_dir,
            project_id=self.id,
            rich_logging=rich_logging,
            log_file=log_file,
            run_id=run_id,
            run_slug=run_slug,
            tasks_dir=self.tasks_dir,
            task_ids=task_ids,
            runtime_ctx=self.ctx,
            connectors=self.connectors,
            concurrency=self.concurrency,
            package_lookups=self.package_lookups,
            all_tasks_upstream=all_tasks_upstream,
            all_tasks_downstream=all_tasks_downstream,
            on_success=on_success + self.on_success,
            on_failure=on_failure + self.on_failure,
            full_refresh=full_refresh,
        )
        runner.run()
