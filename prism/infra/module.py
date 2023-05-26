"""
Prism Module class

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

# Prism-specific imports
import prism.exceptions
from prism.infra.task_manager import PrismTaskManager
from prism.infra.hooks import PrismHooks
from prism.infra.manifest import ModuleManifest
from prism.parsers.ast_parser import AstParser


#####################
# Functions / utils #
#####################

def get_task_var_name(module_path: Path) -> str:
    """
    Retrieve the variable used to store the PrismTask in `module_path` in our namespace

    args:
        module_path: path to module, relative to `modules/`
    returns:
        variable name
    """
    task_var_name = str(module_path).replace('/', '_').replace('.py', '')
    return f"__PRISM_{task_var_name.upper()}__"


####################
# Class definition #
####################

class CompiledModule:
    """
    Class for defining and executing a single compiled module
    """

    def __init__(self,
        module_relative_path: Path,
        module_full_path: Path,
        module_manifest: ModuleManifest
    ):
        self.module_relative_path = module_relative_path
        self.module_full_path = module_full_path
        with open(self.module_full_path, 'r') as f:
            self.module_str = f.read()
        f.close()

        # Module as an AST
        parent_path = Path(str(module_full_path).replace(str(module_relative_path), ''))
        self.ast_parser = AstParser(self.module_relative_path, parent_path)

        # Module name
        self.name = str(self.module_relative_path)

        # Set manifest
        self.module_manifest = module_manifest
        self.refs = self._check_manifest(self.module_manifest)

    def _check_manifest(self, module_manifest: ModuleManifest):
        """
        Check manifest and return list of refs associated with compiled
        module
        """
        refs = []
        manifest_refs = module_manifest.manifest_dict["refs"]
        for ref_obj in manifest_refs:
            refs.append(ref_obj["source"])
        if len(refs) == 1:
            refs = refs[0]
        return refs

    def grab_retries_metadata(self):
        """
        Grab retry metadata, including:
            1. How many retries to undertake
            2. The delay between retries
        """
        prism_task_node = self.ast_parser.get_prism_task_node(
            self.ast_parser.classes, self.ast_parser.bases
        )

        # Instantiate retries / retry_delay_seconds
        retries = None
        retry_delay_seconds = None

        # If the task is a class, the variables will be stored in class attributes
        if isinstance(prism_task_node, ast.ClassDef):
            retries = self.ast_parser.get_variable_assignments(
                self.ast_parser.ast_module, 'RETRIES'
            )
            retry_delay_seconds = self.ast_parser.get_variable_assignments(
                self.ast_parser.ast_module, 'RETRY_DELAY_SECONDS'
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
                    retries = int(kw.value.value)

                if kw.arg == "retry_delay_seconds":
                    if not (
                        isinstance(kw.value, ast.Constant)
                        or isinstance(kw.value, ast.Num)  # noqa: W503
                    ):
                        raise prism.exceptions.RuntimeException(
                            "invalid `retries` keyword...should be an integer"
                        )
                    retry_delay_seconds = int(kw.value.value)

        # If nothing was found, default to 0
        if retries is None:
            retries = 0
        if retry_delay_seconds is None:
            retry_delay_seconds = 0
        return retries, retry_delay_seconds

    def instantiate_module_class(self,
        run_context: Dict[Any, Any],
        task_manager: PrismTaskManager,
        hooks: PrismHooks,
        explicit_run: bool = True,
        user_context: Dict[Any, Any] = {}
    ):
        """
        Instantiate PrismTask child from module

        args:
            run_context: globals dictionary
            task_manager: PrismTaskManager object
            hooks: PrismHooks object
            explicit run: boolean indicating whether to run the Task. Default is True
        returns:
            variable used to store task instantiation
        """
        # Get prism class from module
        prism_task_class = self.ast_parser.get_prism_task_node(
            self.ast_parser.classes, self.ast_parser.bases
        )

        # Both cannot be null
        if prism_task_class is None:
            raise prism.exceptions.ParserException(
                message=f"no PrismTask in `{str(self.module_relative_path)}`"
            )

        # Execute class definition and create task
        exec(self.module_str, run_context)

        # Variable name should just be the name of the module itself (with a bit of
        # styling to prevent accidently exposing variables to users).
        task_var_name = get_task_var_name(self.module_relative_path)

        # If the user specified a task, great!
        if isinstance(prism_task_class, ast.ClassDef):
            prism_task_class_name = prism_task_class.name

            # Execute class definition and create task
            run_context[task_var_name] = run_context[prism_task_class_name](explicit_run)  # noqa: E501

            # Set task manager and hooks
            run_context[task_var_name].set_task_manager(task_manager)
            run_context[task_var_name].set_hooks(hooks)

        # If the user used a decorator, then executing the function will produce the
        # task we want.
        else:
            fn = run_context[prism_task_class.name]
            if fn.__name__ != "wrapper_task":
                raise prism.exceptions.RuntimeException(
                    "`task` decorator not properly specified...try adding parentheses to it, e.g., `@task()`"  # noqa: E501
                )
            task = fn(task_manager, hooks)
            task.bool_run = explicit_run

            run_context[task_var_name] = task

        # Return name of variable used to store task instantiation
        return task_var_name

    def exec(self,
        run_context: Dict[Any, Any],
        task_manager: PrismTaskManager,
        hooks: PrismHooks,
        explicit_run: bool = True,
        user_context: Dict[Any, Any] = {}
    ) -> PrismTaskManager:
        """
        Execute module
        """
        task_var_name = self.instantiate_module_class(
            run_context, task_manager, hooks, explicit_run, user_context
        )

        # Execute the task
        run_context[task_var_name].exec()
        task_manager.upstream[self.name] = run_context[task_var_name]
        return task_manager
