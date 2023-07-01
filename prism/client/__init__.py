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
import argparse
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

# Prism-specific imports
import prism.constants
import prism.cli.base
import prism.exceptions
from prism.infra import executor as prism_executor
from prism.infra import model as prism_model
from prism.infra import compiler as prism_compiler
import prism.mixins.base
import prism.mixins.compile
import prism.mixins.connect
import prism.mixins.run
import prism.mixins.sys_handler
from prism.parsers import ast_parser
import prism.prism_logging


####################
# Class definition #
####################

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

        # Models directory
        self.models_dir = self.get_models_dir(self.project_dir)

        # All models in project
        self.all_models = self.get_models(self.models_dir)

        # Define run context
        self.run_context = prism.constants.CONTEXT.copy()

        # Set up default logger
        args = argparse.Namespace()
        args.log_level = self.log_level
        prism.prism_logging.set_up_logger(args)

    def _is_valid_project(self, user_project_dir: Path) -> bool:
        """
        Determine if `user_project_dir` is a valid project (i.e., that is has a
        `prism_project.py` file and a `models` folder)

        args:
            user_project_dir: project path
        raises:
            exception if Prism project is not valid
        """
        os.chdir(user_project_dir)
        temp_project_dir = prism.cli.base.get_project_dir()

        # Inputted project directory is not the same as the computed project directory
        if temp_project_dir != user_project_dir:
            msg = f'no project at `{str(user_project_dir)}, closest project found at `{str(temp_project_dir)}`'   # noqa: E501
            raise prism.exceptions.InvalidProjectException(message=msg)

        # Models folder is not found
        if not Path(user_project_dir / 'models').is_dir():
            raise prism.exceptions.InvalidProjectException(
                message=f'`models` directory not found in `{str(user_project_dir)}`'
            )

        return True

    def connect(self,
        connection_type: str,
        user_context: Optional[Dict[str, Any]] = None
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
            msg = f'invalid connection type `{connection_type}; must be one of {",".join(prism.constants.VALID_ADAPTERS)}'  # noqa: E501
            raise prism.exceptions.RuntimeException(message=msg)

        # Create the project. We need this to grab the profile YML path.
        prism_project = self.create_project(
            project_dir=self.project_dir,
            user_context={} if user_context is None else user_context,
            which="connect",
            filename="prism_project.py",
        )

        # Get the profile YML path and create the connection.
        profile_yml_path = prism_project.profile_yml_path

        # Try to create the connection. If we encounter an error, then cleanup first and
        # then raise the exception.
        self.run_context = prism_project.run_context
        try:
            # mypy doesn't recognize that we override profile_yml_path if it is None.
            self.create_connection(connection_type, profile_yml_path)  # type: ignore
        except Exception as e:
            raise e
        finally:
            self.run_context = prism_project.cleanup(self.run_context)

    def compile(self,
        models: Optional[List[str]] = None,
        all_downstream: bool = True
    ) -> prism_compiler.CompiledDag:
        """
        Compile the Prism project
        """
        if models is None:
            model_paths = self.all_models
        else:
            model_paths = [Path(p) for p in models]
        self.user_arg_models_list = model_paths

        # Create compiled directory and compile the DAG
        compiled_dir = self.create_compiled_dir(self.project_dir)
        self.compiled_dir = compiled_dir
        return self.compile_dag(
            self.project_dir,
            self.compiled_dir,
            self.all_models,
            self.user_arg_models_list,
            all_downstream
        )

    def run(self,
        models: Optional[List[str]] = None,
        all_upstream: bool = True,
        all_downstream: bool = False,
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

        # Run sys.path engine
        sys_path_engine = prism_project.sys_path_engine
        self.run_context = sys_path_engine.modify_sys_path(
            prism_project.sys_path_config
        )

        # Compile the DAG
        compiled_dag = self.compile(models, all_downstream)

        # Create DAG executor and Pipeline objects
        threads = prism_project.thread_count
        dag_executor = prism_executor.DagExecutor(
            self.project_dir,
            compiled_dag,
            all_upstream,
            all_downstream,
            threads,
            user_context
        )
        pipeline = self.create_pipeline(
            prism_project, dag_executor, self.run_context
        )

        # Create args and exec
        output = pipeline.exec(full_tb)

        # Cleanup
        self.run_context = prism_project.cleanup(
            self.run_context
        )

        # Write error
        if output.error_event is not None:
            event = output.error_event
            try:
                # Exception is a PrismException
                exception = event.err
            except AttributeError:
                # All other exceptions
                exception = event.value
            raise exception

    def _get_task_cls_from_namespace(self, model_path: Path):
        """
        Get PrismTask associated with `model_path` from Prism namespace

        args:
            model_path: path to model (relative to `models/` folder)
        returns:
            task class
        """

        # Name of variable containing the PrismTask for `model_path`
        task_var_name = prism_model.get_task_var_name(model_path)
        task_cls = self.run_context.get(task_var_name)

        # If class is None, then raise an error. Otherwise, return the class
        if task_cls is None:
            msg_list = [
                f'no output found for `{str(model_path)}` in the PrismProject namespace',                     # noqa: E501
                f'check that `{str(model_path)}` has a PrismTask and that `{str(model_path)}` has been run'  # noqa: E501
            ]
            raise prism.exceptions.RuntimeException(message='\n'.join(msg_list))
        return task_cls

    def get_task_output(self,
        model_path: Path,
        bool_run: bool = False,
        **kwargs
    ) -> Any:
        """
        Get output of the task in `model`. If bool_run==True, then run the project
        first. Note that if bool_run==False, then only tasks with a target can be
        parsed.

        args:
            model: path to model (relative to `models/` folder)
            bool_run: boolean indicating whether to run pipeline first; default is False
            **kwargs: keyword arguments for running
        returns:
            task output
        """
        # The user may have run the project in their script. Try retrieving the output
        try:
            task_cls = self._get_task_cls_from_namespace(model_path)
            return task_cls.get_output()

        except prism.exceptions.RuntimeException:
            # If project is run, then any output can be retrieved from any task
            if bool_run:
                self.run(**kwargs)
                task_cls = self._get_task_cls_from_namespace(model_path)
                return task_cls.get_output()

            # If project is not run, then only targets can be retrieved
            else:
                parsed_ast_model = ast_parser.AstParser(model_path, self.models_dir)
                prism_task_cls = parsed_ast_model.get_prism_task_node(
                    parsed_ast_model.classes, parsed_ast_model.bases
                )
                if prism_task_cls is None:
                    return None
                prism_task_cls_name = prism_task_cls.name

                # We need to update sys.path to include all paths in SYS_PATH_CONF,
                # since some target locations may depend on vars stored in models
                # imported from directories contained therein.
                user_context = kwargs.get("user_context", {})
                prism_project = self.create_project(
                    project_dir=self.project_dir,
                    user_context=user_context,
                    which="run",
                    filename="prism_project.py"
                )

                # Run sys.path engine
                sys_path_engine = prism_project.sys_path_engine
                self.run_context = sys_path_engine.modify_sys_path(
                    prism_project.sys_path_config
                )

                # Execute the class definition code
                exec(parsed_ast_model.model_str, self.run_context)
                task = self.run_context[prism_task_cls_name](False)  # type: ignore # noqa: E501

                # No need for an actual task_manager/hooks, since we're only accessing
                # the target
                try:
                    task.set_hooks(None)
                    task.set_task_manager(None)
                    task.exec()
                    output = task.get_output()

                    # Cleanup
                    self.run_context = prism_project.cleanup(
                        self.run_context
                    )
                    return output

                # If there is some error when executing the task, then cleanup first and
                # then raise the exception.
                except Exception as e:
                    self.run_context = prism_project.cleanup(
                        self.run_context
                    )
                    raise e

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
