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
import re
from typing import Any, Dict, List, Optional

# Prism-specific imports
import prism.constants
import prism.cli.base
import prism.exceptions
from prism.infra import executor as prism_executor
from prism.infra import compiler as prism_compiler
from prism.infra.compiler import CompiledDag
from prism.infra.model import CompiledTask
import prism.mixins.base
import prism.mixins.compile
import prism.mixins.connect
import prism.mixins.run
import prism.mixins.sys_handler
import prism.prism_logging
import prism.cli.run
from prism.triggers import TriggerManager


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

        # Set up default logger
        self.args = argparse.Namespace()
        self.args.log_level = self.log_level
        prism.prism_logging.set_up_logger(self.args)

        # Check if project is valid
        self._is_valid_project(self.project_dir)

        # Tasks directory
        self.tasks_dir = self.get_tasks_dir(self.project_dir)

        # All tasks in project
        self.all_modules = self.get_modules(self.tasks_dir)
        self.all_tasks = self.parse_all_tasks(
            all_modules=self.all_modules,
            tasks_dir=self.tasks_dir
        )

        # Define run context
        self.run_context = prism.constants.CONTEXT.copy()

        # Store outputs of tasks
        self.task_outputs = {}

    def _is_valid_project(self, user_project_dir: Path) -> bool:
        """
        Determine if `user_project_dir` is a valid project (i.e., that is has a
        `prism_project.py` file and a `tasks` folder)

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

        # Tasks folder is not found
        if not Path(user_project_dir / 'tasks').is_dir():

            # If the `modules` directory is found, then raise a warning
            if Path(user_project_dir / 'modules').is_dir():
                prism.prism_logging.fire_console_event(
                    prism.prism_logging.ModulesFolderDeprecated(),
                    log_level='warn'
                )

            # Otherwise, raise an error
            else:
                raise prism.exceptions.InvalidProjectException(
                    message=f'`tasks` directory not found in `{str(user_project_dir)}`'
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
        tasks: Optional[List[str]] = None,
        all_downstream: bool = True
    ) -> prism_compiler.CompiledDag:
        """
        Compile the Prism project
        """
        # Prism project
        prism_project = self.create_project(
            project_dir=self.project_dir,
            user_context={},
            which="compile",
            filename="prism_project.py"
        )
        self.run_context = prism_project.run_context

        # Construct list of all tasks
        self.args.tasks = tasks
        user_arg_tasks = self.user_arg_tasks(self.args, self.tasks_dir, self.all_tasks)

        # Create compiled directory
        compiled_dir = self.create_compiled_dir(self.project_dir)
        self.compiled_dir = compiled_dir

        # Try to compile the DAG. If we encounter an error, then cleanup first and then
        # raise the exception.
        try:
            compiled_dag = self.compile_dag(
                project_dir=self.project_dir,
                tasks_dir=self.tasks_dir,
                compiled_dir=compiled_dir,
                all_parsed_tasks=self.all_tasks,
                user_arg_tasks=user_arg_tasks,
                user_arg_all_downstream=all_downstream,
                project=prism_project
            )
        except Exception as e:
            raise e
        finally:
            self.run_context = prism_project.cleanup(self.run_context)

        # Return
        self.compiled_dag = compiled_dag
        return compiled_dag

    def run(self,
        tasks: Optional[List[str]] = None,
        all_upstream: bool = True,
        all_downstream: bool = False,
        full_tb: bool = True,
        user_context: Optional[Dict[str, Any]] = None
    ):
        """
        Run the Prism project
        """
        # Create Prism project
        if user_context is None:
            user_context = {}
        prism_project = self.create_project(
            project_dir=self.project_dir,
            user_context=user_context,
            which="run",
            filename="prism_project.py",
        )
        self.run_context = prism_project.run_context

        # Wrap everything in a try-except block. If any error occurs, cleanup the Prism
        # project before re-running anything
        try:
            # Run sys.path engine
            sys_path_engine = prism_project.sys_path_engine
            self.run_context = sys_path_engine.modify_sys_path(
                prism_project.sys_path_config
            )

            # Prepare triggers
            triggers_yml_path = prism_project.triggers_yml_path
            trigger_manager = TriggerManager(
                triggers_yml_path,
                prism_project,
            )

            # Construct list of all tasks
            self.args.tasks = tasks
            user_arg_tasks = self.user_arg_tasks(
                self.args,
                self.tasks_dir,
                self.all_tasks
            )

            # Create compiled directory
            self.compiled_dir = self.create_compiled_dir(self.project_dir)

            # Compile the DAG
            self.compiled_dag = self.compile_dag(
                project_dir=self.project_dir,
                tasks_dir=self.tasks_dir,
                compiled_dir=self.compiled_dir,
                all_parsed_tasks=self.all_tasks,
                user_arg_tasks=user_arg_tasks,
                user_arg_all_downstream=all_downstream,
                project=prism_project
            )

            # Create DAG executor and Pipeline objects
            threads = prism_project.thread_count
            dag_executor = prism_executor.DagExecutor(
                self.project_dir,
                self.compiled_dag,
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

            # Write error
            if output.error_event is not None:
                event = output.error_event
                try:
                    # Exception is a PrismException
                    exception = event.err
                except AttributeError:
                    # All other exceptions
                    exception = event.value

                # Fire the `on_failure` events
                trigger_manager.exec(
                    'on_failure',
                    full_tb,
                    [],
                    self.run_context,
                )
                raise exception

            # Otherwise, fire the `on_success` events
            trigger_manager.exec(
                'on_success',
                full_tb,
                [],
                self.run_context,
            )

            # Store the outputs of the various tasks
            for task in user_arg_tasks:
                task_instance = self.run_context[task]
                self.task_outputs[task] = task_instance.get_output()

            # Cleanup
            self.run_context = prism_project.cleanup(
                self.run_context
            )

        # If we encounter any exception, then cleanup first and then raise
        except Exception:
            self.run_context = prism_project.cleanup(
                self.run_context
            )
            raise

    def clear_task_output(self):
        self.task_outputs = {}

    def get_task_output(self,
        task_name: str,
        bool_run: bool = False,
        **kwargs
    ) -> Any:
        """
        Get output of the task in `task`. If bool_run==True, then run the project
        first. Note that if bool_run==False, then only tasks with a target can be
        parsed.

        args:
            task_name: either `<module_name>` or `<module_name>.<task_name>`. Note that
                if the user inputs the former, then the module should only have one
                task.
            bool_run: boolean indicating whether to run pipeline first; default is False
            **kwargs: keyword arguments for running
        returns:
            task output
        """
        # Remove the `.py` from the ending of the task name, if it exists
        task_name = re.sub(r'\.py$', '', task_name)

        # Process the task name
        tmp_module_name = task_name.split(".")[0]
        tmp_task_name = None
        try:
            tmp_task_name = task_name.split(".")[1]
        except IndexError:
            for _p in self.all_tasks:
                if Path(f"{tmp_module_name}.py") == _p.task_relative_path:
                    if len(_p.prism_task_names) == 0:
                        raise prism.exceptions.ParserException(
                            message=f"no PrismTask in `{str(_p.task_relative_path)}`"
                        )
                    if len(_p.prism_task_names) > 1:
                        raise prism.exceptions.RuntimeException(
                            message=f"module `{tmp_module_name}` has multiple tasks...specify the task name and try again"  # noqa: E501
                        )
                    tmp_task_name = _p.prism_task_names[0]

        # If task name is still null, then something when wrong
        if tmp_task_name is None:
            raise prism.exceptions.RuntimeException(
                message=f"could not find any task with module `{tmp_module_name}`"
            )
        processed_task_name = f"{tmp_module_name}.{tmp_task_name}"

        # The user may have run the project in their script. In that case, the PrismDAG
        # instance will have a `compiled_dag` attribute that stores an instance of the
        # `CompiledDag` class.
        try:
            return self.task_outputs[processed_task_name]

        except KeyError:
            # If project is run, then any output can be retrieved from any task
            if bool_run:
                self.run(**kwargs)
                return self.task_outputs[processed_task_name]

            # If project is not run, then only targets can be retrieved
            else:

                # We need to compile the project
                compiled_dag: CompiledDag = self.compile()

                # Get the task associated with `processed_task_name`
                compiled_task: CompiledTask
                for _t in compiled_dag.compiled_tasks:
                    if _t.task_var_name == processed_task_name:
                        compiled_task = _t

                # We need to update sys.path to include all paths in SYS_PATH_CONF,
                # since some target locations may depend on vars stored in tasks
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
                try:
                    task_var_name = compiled_task.instantiate_task_class(
                        run_context=self.run_context,
                        task_manager=None,
                        hooks=None,
                        explicit_run=False
                    )
                    task_cls = self.run_context[task_var_name]
                    task_cls.exec()
                    output = task_cls.get_output()

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
