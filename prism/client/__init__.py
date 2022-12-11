"""
PrismDAG class definition. This class can be used to manipulate Prism projects / tasks
without the CLI

Table of Contents
- Imports
- Functions / utils
- Class definition
"""


###########
# Imports #
###########

# Standard library imports
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

# Prism-specific imports
import prism.constants
import prism.cli.base
import prism.exceptions
from prism.infra import executor as prism_executor
from prism.infra import module as prism_module
from prism.infra import compiler as prism_compiler
from prism.infra.project import PrismProject
import prism.mixins.base
import prism.mixins.compile
import prism.mixins.connect
import prism.mixins.run
import prism.mixins.sys_handler
from prism.parsers import ast_parser
import prism.logging


####################
# Class definition #
####################

@dataclass
class LoggingArgs:
    log_level: str


class PrismDAG(
    prism.mixins.base.BaseMixin,
    prism.mixins.compile.CompileMixin,
    prism.mixins.connect.ConnectMixin,
    prism.mixins.run.RunMixin,
    prism.mixins.sys_handler.SysHandlerMixin
):
    """
    Class to access Prism infrastructure without the CLI
    """

    def __init__(self,
        project_dir: Path,
        log_level: str = 'warn'
    ):
        self.project_dir = project_dir
        self.log_level = log_level

        # Set the project to None.

        # Check if project is valid
        self._is_valid_project(self.project_dir)

        # Modules directory
        self.modules_dir = self.get_modules_dir(self.project_dir)

        # All modules in project
        self.all_modules = self.get_modules(self.modules_dir)

        # Define run context
        self.run_context = prism.constants.CONTEXT.copy()

        # Set up default logger
        prism.logging.set_up_logger(LoggingArgs(self.log_level))

    def _is_valid_project(self, user_project_dir: Path) -> bool:
        """
        Determine if `user_project_dir` is a valid project (i.e., that is has a
        `prism_project.py` file and a `modules` folder)

        args:
            user_project_dir: project path
        raises:
            exception if Prism project is not valid
        """
        os.chdir(user_project_dir)
        temp_project_dir = prism.cli.base.get_project_dir()

        # Inputted project directory is not the same as the computed project directory
        if temp_project_dir != user_project_dir:
            msg_list = [
                f'no project at `{str(user_project_dir)}',
                f'closest project found at `{str(temp_project_dir)}`'
            ]
            raise prism.exceptions.InvalidProjectException(message='\n'.join(msg_list))

        # Modules folder is not found
        if not Path(user_project_dir / 'modules').is_dir():
            raise prism.exceptions.InvalidProjectException(
                message=f'`modules` directory not found in `{str(user_project_dir)}`'
            )

        return True

    def connect(self,
        connection_type: str
    ):
        """
        Connect task to an adapter
        """
        # Parse arguments
        if connection_type is None:
            raise prism.exceptions.RuntimeException(
                message='connection type cannot be None'
            )
        if not isinstance(connection_type, str):
            raise prism.exceptions.RuntimeException(
                message='connection type must be a string'
            )
        if connection_type not in prism.constants.VALID_ADAPTERS:
            msg_list = [
                f'invalid connection type `{connection_type}',
                f'must be one of {",".join(prism.constants.VALID_ADAPTERS)}'
            ]
            raise prism.exceptions.RuntimeException(message='\n'.join(msg_list))

        # Get profiles filepath
        prism_project = PrismProject(
            project_dir=self.project_dir,
            user_context={},
            which="connect",
            filename="prism_project.py"
        )
        profiles_dir = prism_project.project_dir
        profiles_filepath = profiles_dir / 'profile.yml'
        self.create_connection(connection_type, profiles_filepath)

    def compile(self,
        modules: Optional[List[str]] = None
    ) -> prism_compiler.CompiledDag:
        """
        Compile the Prism project
        """
        if modules is None:
            module_paths = self.all_modules
        else:
            module_paths = [Path(p) for p in modules]
        self.user_arg_modules = module_paths

        # Create compiled directory and compile the DAG
        compiled_dir = self.create_compiled_dir(self.project_dir)
        self.compiled_dir = compiled_dir
        return self.compile_dag(
            self.project_dir, self.compiled_dir, self.all_modules, self.user_arg_modules
        )

    def run(self,
        modules: Optional[List[str]] = None,
        all_upstream: bool = True,
        full_tb: bool = True,
        user_context: Optional[Dict[str, Any]] = None
    ):
        """
        Run the Prism project
        """

        # Create PrismProject
        if user_context is None:
            user_context = {}
        prism_project = self.create_project(
            project_dir=self.project_dir,
            user_context=user_context,
            which="run",
            filename="prism_project.py",
        )

        # Compile the DAG
        compiled_dag = self.compile(modules)
        
        # Create DAG executor and Pipeline objects
        threads = prism_project.thread_count
        dag_executor = prism_executor.DagExecutor(
            self.project_dir, compiled_dag, all_upstream, threads
        )
        pipeline = self.create_pipeline(
            prism_project, dag_executor, self.run_context
        )

        # Create args and exec
        output = pipeline.exec(full_tb)
        if output.error_event is not None:
            event = output.error_event
            try:
                # Exception is a PrismException
                exception = event.err
            except AttributeError:
                # All other exceptions
                exception = event.value
            raise exception

    def _get_task_cls_from_namespace(self, module_path: Path):
        """
        Get PrismTask associated with `module_path` from Prism namespace

        args:
            module_path: path to module (relative to `modules/` folder)
        returns:
            task class
        """

        # Name of variable containing the PrismTask for `module_path`
        task_var_name = prism_module.get_task_var_name(module_path)
        task_cls = self.run_context.get(task_var_name)

        # If class is None, then raise an error. Otherwise, return the class
        if task_cls is None:
            msg_list = [
                f'no output found for `{str(module_path)}` in the PrismProject namespace',                     # noqa: E501
                f'check that `{str(module_path)}` has a PrismTask and that `{str(module_path)}` has been run'  # noqa: E501
            ]
            raise prism.exceptions.RuntimeException(message='\n'.join(msg_list))
        return task_cls

    def get_task_output(self,
        module_path: Path,
        bool_run: bool = False,
        **kwargs
    ) -> Any:
        """
        Get output of the task in `module`. If bool_run==True, then run the project
        first. Note that if bool_run==False, then only tasks with a target can be
        parsed.

        args:
            module: path to module (relative to `modules/` folder)
            bool_run: boolean indicating whether to run pipeline first; default is False
            **kwargs: keyword arguments for running
        returns:
            task output
        """

        # The user may have run the project in their script. Try retrieving the output
        try:
            task_cls = self._get_task_cls_from_namespace(module_path)
            return task_cls.get_output()

        except prism.exceptions.RuntimeException:
            # If project is run, then any output can be retrieved from any task
            if bool_run:
                self.run(**kwargs)
                task_cls = self._get_task_cls_from_namespace(module_path)
                return task_cls.get_output()

            # If project is not run, then only targets can be retrieved
            else:
                parsed_ast_module = ast_parser.AstParser(module_path, self.modules_dir)
                prism_task_cls = parsed_ast_module.get_prism_task_node(
                    parsed_ast_module.classes, parsed_ast_module.bases
                )
                prism_task_cls_name = prism_task_cls.name
                task_var_name = prism_module.get_task_var_name(module_path)

                # We need to update sys.path to include all paths in SYS_PATH_CONF,
                # since some target locations may depend on vars stored in modules
                # imported from directories contained therein.
                user_context = kwargs.get("user_context", {})
                prism_project = self.create_project(
                    project_dir=self.project_dir,
                    user_context=user_context,
                    which="run",
                    filename="prism_project.py"
                )
                self.add_paths_to_sys_path(
                    prism_project.sys_path_config,
                    prism_project.run_context
                )

                # Execute the class definition code
                exec(parsed_ast_module.module_str, prism_project.run_context)
                task = prism_project.run_context[prism_task_cls_name](False)  # noqa: E501 do NOT run the task
                
                # No need for an actual task_manager/hooks, since we're only accessing
                # the target
                task.set_hooks(None)
                task.set_task_manager(None)
                task.exec()
                output = task.get_output()
                return output

    def get_pipeline_output(self, bool_run: bool = False) -> Any:
        """
        Get pipeline output, defined as the output associated with the last task in the
        project

        args:
            bool_run: boolean indicating whether to run the pipeline
        returns:
            output associated with last task in the project
        """
        # Compile the project and get the last task
        compiled_dag = self.compile()
        last_task = compiled_dag.topological_sort[-1]
        return self.get_task_output(last_task, bool_run)
