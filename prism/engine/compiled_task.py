import ast
import importlib
import re
from typing import List, Literal, Optional, Tuple, Union

# Prism-specific imports
from prism.db.mixins import DbMixin
import prism.exceptions
from prism.engine.module import _PrismModule
from prism.task import PrismTask


class _CompiledTask(DbMixin):
    """
    Class for defining and executing a single compiled task
    """

    run_slug: Optional[str]
    task_id: str
    task_module: _PrismModule
    refs: List[str]

    prism_task_node: Union[ast.ClassDef, ast.FunctionDef]

    def __init__(
        self,
        run_slug: Optional[str],
        task_id: str,
        task_module: _PrismModule,
        refs: List[str],
    ):
        self.run_slug = run_slug
        self.task_id = task_id
        self.task_module = task_module
        self.refs = refs

        # Define the task node
        try:
            self.prism_task_node = task_module.prism_task_nodes[self.task_id]
        except KeyError:
            raise prism.exceptions.ParserException(
                message=f"could not find task `{self.task_id}` in `{self.task_module.module_task_relpath}`"  # noqa: E501
            )

    def grab_retries_metadata(self) -> Tuple[int, int]:
        """
        Grab retry metadata, including:
            1. How many retries to undertake
            2. The delay between retries
        """
        # Instantiate retries / retry_delay_seconds
        retries = None
        retry_delay_seconds = None

        # If the task is a class, the variables will be stored in class attributes
        if isinstance(self.prism_task_node, ast.ClassDef):
            retries = self.task_module.get_class_attribute_value(
                self.prism_task_node, "retries"
            )
            retry_delay_seconds = self.task_module.get_class_attribute_value(
                self.prism_task_node, "retry_delay_seconds"
            )
            if retries:
                if not isinstance(retries, int):
                    raise ValueError("TypeError: `retries` should be an integer!")
            if retry_delay_seconds:
                if not isinstance(retry_delay_seconds, int):
                    raise ValueError(
                        "TypeError: `retry_delay_seconds` should be an integer!"
                    )

        # If the task is a decorated function, the variables will be stored as keyword
        # arguments.
        elif isinstance(self.prism_task_node, ast.FunctionDef):
            decorator_call = self.task_module.get_task_decorator_call(
                self.prism_task_node
            )
            retries = self.task_module._get_keyword_arg_from_task_decorator(
                self.prism_task_node, decorator_call, "retries", "int"
            )
            retry_delay_seconds = self.task_module._get_keyword_arg_from_task_decorator(
                self.prism_task_node, decorator_call, "retry_delay_seconds", "int"
            )

        # If nothing was found, default to 0
        if not retries:
            retries = 0
        if not retry_delay_seconds:
            retry_delay_seconds = 0

        # Hacky, but good enough for now
        assert isinstance(retries, int)
        assert isinstance(retry_delay_seconds, int)

        return retries, retry_delay_seconds

    def instantiate_task_class(self, explicit_run: bool = True) -> PrismTask:
        """
        Instantiate the PrismTask class. All tasks, even decorated functions, are
        converted to a PrismTask class before being executed.

        args:
            explicit run: boolean indicating whether to run the task. Default is True
        returns:
            PrismTask instance
        """
        project_relpath = self.task_module.module_task_relpath

        # Before replacing forward slashes with periods, make sure any leading slashes
        # are removed.
        project_relpath_for_import = re.sub(
            r"(^\.+)/(.+)|^/(.+)",
            r"\1\2\3",
            project_relpath,
        )

        # If we attempt a relative import that is beyond the top-level package, we
        # encounter an error. To handle this,
        import_statement = project_relpath_for_import.replace(".py", "").replace(
            "/", "."
        )
        imported_mod = importlib.import_module(import_statement)

        # Get the node name
        node_name = self.prism_task_node.name
        if isinstance(self.prism_task_node, ast.FunctionDef):
            task_class_fn: PrismTask = getattr(imported_mod, node_name)(
                task_id=self.task_id,
                bool_run=explicit_run,
            )
            return task_class_fn
        else:
            task_class_cls: PrismTask = getattr(imported_mod, node_name)(
                task_id=self.task_id,
                bool_run=explicit_run,
            )
            return task_class_cls

    def exec(self, explicit_run: bool = True, full_refresh: bool = False) -> PrismTask:
        """
        Execute task
        """
        # Add TaskRun to the database
        assert self.run_slug is not None
        super().create_task_run(self.run_slug, self.task_id)

        runtime = importlib.import_module("prism.runtime")
        # TODO: clean up this error
        if not hasattr(runtime, "CurrentRun"):
            raise ValueError("runtime does not have `CurrentRun` attribute!")

        # Instantiate class and check if the task is already done
        prism_task = self.instantiate_task_class(explicit_run)
        is_done = prism_task.done()
        prism_task.is_done = is_done and not full_refresh

        # Execute the task
        # "PENDING", "RUNNING", "SUCCEEDED", "FAILED", "SKIPPED"
        status: Literal["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "SKIPPED"] = (
            "SKIPPED" if prism_task.is_done or not explicit_run else "RUNNING"
        )  # noqa: E501
        super().update_task_run_status(self.run_slug, prism_task.task_id, status)
        prism_task.exec()
        if status == "RUNNING":
            super().update_task_run_status(
                self.run_slug, prism_task.task_id, "SUCCEEDED"
            )
        runtime.CurrentRun._set_task_output_value(self.task_id, prism_task.get_output())

        # Return the task... we don't use the task for anything, but it helps our event
        # manager know that we ran a task.
        return prism_task
