"""
Prism Model class

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Standard library imports
import ast
from pathlib import Path
from typing import Any, Dict
import re

# Prism-specific imports
import prism.exceptions
from prism.infra.task_manager import PrismTaskManager
from prism.infra.hooks import PrismHooks
from prism.infra.manifest import ModelManifest
from prism.parsers.ast_parser import AstParser


####################
# Class definition #
####################

class CompiledModel:
    """
    Class for defining and executing a single compiled model
    """

    def __init__(self,
        model_name: str,
        model_relative_path: Path,
        model_full_path: Path,
        model_manifest: ModelManifest,
        model_ast_parser: AstParser,
    ):
        self.model_name = model_name
        self.model_relative_path = model_relative_path
        self.model_full_path = model_full_path
        with open(self.model_full_path, 'r') as f:
            self.model_str = f.read()
        f.close()

        # # Model as an AST
        self.ast_parser = model_ast_parser

        # Model name
        self.name = re.sub(r'\.py$', '', str(self.model_relative_path))

        # Set manifest
        self.model_manifest = model_manifest
        self.refs = self.model_manifest.manifest_dict["refs"][self.name][self.model_name]  # noqa: E501

        # Task var name
        self.task_var_name = f"{self.name}.{self.model_name}"

    def grab_retries_metadata(self):
        """
        Grab retry metadata, including:
            1. How many retries to undertake
            2. The delay between retries
        """
        prism_task_node = [
            _n for _n in self.ast_parser.prism_task_nodes if _n.name == self.model_name
        ][0]

        # Instantiate retries / retry_delay_seconds
        retries = None
        retry_delay_seconds = None

        # If the task is a class, the variables will be stored in class attributes
        if isinstance(prism_task_node, ast.ClassDef):
            retries = self.ast_parser.get_variable_assignments(
                self.ast_parser.ast_model, 'RETRIES'
            )
            retry_delay_seconds = self.ast_parser.get_variable_assignments(
                self.ast_parser.ast_model, 'RETRY_DELAY_SECONDS'
            )

        # If the task is a decorated function, the variables will be stored as keyword
        # arguments.
        elif isinstance(prism_task_node, ast.FunctionDef):

            task_dec_call = self.ast_parser.get_task_decorator_call(prism_task_node)
            for kw in task_dec_call.keywords:
                if kw.arg == "retries":
                    if not (
                        isinstance(kw.value, ast.Constant)
                        or isinstance(kw.value, ast.Num)  # noqa: W503
                    ):
                        raise prism.exceptions.RuntimeException(
                            "invalid `retries` keyword...should be an integer"
                        )

                    if hasattr(kw.value, "value"):
                        retries = int(kw.value.value)
                    else:
                        retries = kw.value.n

                if kw.arg == "retry_delay_seconds":
                    if not (
                        isinstance(kw.value, ast.Constant)
                        or isinstance(kw.value, ast.Num)  # noqa: W503
                    ):
                        raise prism.exceptions.RuntimeException(
                            "invalid `retries` keyword...should be an integer"
                        )

                    if hasattr(kw.value, "value"):
                        retry_delay_seconds = int(kw.value.value)
                    else:
                        retry_delay_seconds = kw.value.n

        # If nothing was found, default to 0
        if retries is None:
            retries = 0
        if retry_delay_seconds is None:
            retry_delay_seconds = 0
        return retries, retry_delay_seconds

    def instantiate_model_class(self,
        run_context: Dict[Any, Any],
        task_manager: PrismTaskManager,
        hooks: PrismHooks,
        explicit_run: bool = True,
        user_context: Dict[Any, Any] = {}
    ):
        """
        Instantiate PrismTask child from model

        args:
            run_context: globals dictionary
            task_manager: PrismTaskManager object
            hooks: PrismHooks object
            explicit run: boolean indicating whether to run the Task. Default is True
        returns:
            variable used to store task instantiation
        """
        # Get prism class from model
        prism_task_node = [
            _n for _n in self.ast_parser.prism_task_nodes if _n.name == self.model_name
        ][0]

        # Both cannot be null
        if prism_task_node is None:
            raise prism.exceptions.ParserException(
                message=f"could not find model `{self.model_name}` in `{str(self.model_relative_path)}`"  # noqa: E501
            )

        # Execute class definition and create task
        exec(self.model_str, run_context)

        # If the user specified a task, great!
        if isinstance(prism_task_node, ast.ClassDef):
            prism_task_node_name = prism_task_node.name

            # Execute class definition and create task
            run_context[self.task_var_name] = run_context[prism_task_node_name](explicit_run)  # noqa: E501

            # Set task manager and hooks
            run_context[self.task_var_name].set_task_manager(task_manager)
            run_context[self.task_var_name].set_hooks(hooks)

        # If the user used a decorator, then executing the function will produce the
        # task we want.
        else:
            fn = run_context[prism_task_node.name]
            if fn.__name__ != "wrapper_task":
                raise prism.exceptions.RuntimeException(
                    "`task` decorator not properly specified...try adding parentheses to it, e.g., `@task()`"  # noqa: E501
                )
            task = fn(task_manager, hooks)
            task.bool_run = explicit_run

            run_context[self.task_var_name] = task

        # Return name of variable used to store task instantiation
        return self.task_var_name

    def exec(self,
        run_context: Dict[Any, Any],
        task_manager: PrismTaskManager,
        hooks: PrismHooks,
        explicit_run: bool = True,
        user_context: Dict[Any, Any] = {}
    ) -> PrismTaskManager:
        """
        Execute model
        """
        task_var_name = self.instantiate_model_class(
            run_context, task_manager, hooks, explicit_run, user_context
        )

        # Execute the task
        run_context[task_var_name].exec()
        task_manager.upstream[self.name] = run_context[task_var_name]
        return task_manager
