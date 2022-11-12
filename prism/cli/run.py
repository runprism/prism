"""
Run task class definition, called via `prism run`

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Standard library imports
import os
from typing import List

# Prism-specific imports
import prism.cli.base
import prism.cli.compile
import prism.mixins.run
import prism.exceptions
import prism.constants
import prism.logging
from prism.logging import Event, fire_console_event, fire_empty_line_event
from prism.event_managers import base as base_event_manager
from prism.infra import executor as prism_executor


####################
# Class definition #
####################

class RunTask(prism.cli.compile.CompileTask, prism.mixins.run.RunMixin):
    """
    Class for defining the "run" task
    """

    def run(self) -> prism.cli.base.TaskRunReturnResult:
        """
        Execute run task. Function is organized as follows:

        - Fire header events
        - Get location of config files, and get modules to compile
        - Create compiled DAG
        - Create prism project
        - Create pipeline
        - Execute pipeline
        - Fire footer events
        """

        # Keep track of events
        event_list: List[Event] = []

        # ------------------------------------------------------------------------------
        # Fire header events

        event_list, project_dir = self.fire_header_events(event_list)
        if project_dir is None:
            return prism.cli.base.TaskRunReturnResult(event_list)
        os.chdir(project_dir)

        # Compiled dir
        compiled_dir = self.create_compiled_dir(project_dir)

        # ------------------------------------------------------------------------------
        # Get location of config files, and get modules to compile

        profiles_path = self.get_profile_path(self.args, project_dir)

        # Get modules to compile
        try:
            modules_dir = self.get_modules_dir(project_dir)
        except prism.exceptions.CompileException as err:
            e = prism.logging.PrismExceptionErrorEvent(
                err,
                'accessing modules directory'
            )
            event_list = fire_console_event(e, event_list, 0, log_level='error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)
        user_arg_modules = self.user_arg_modules(self.args, modules_dir)
        event_list = fire_console_event(
            prism.logging.CompileStartEvent(len(user_arg_modules), 'execute'),
            event_list,
            log_level='info'
        )
        event_list = fire_empty_line_event(event_list)

        # ------------------------------------------------------------------------------
        # Create compiled DAG

        result = super().run_for_subclass(
            self.args,
            project_dir,
            compiled_dir,
            event_list,
            True
        )
        if isinstance(result, prism.cli.base.TaskRunReturnResult):
            return result

        compiled_dag = result.outputs
        compiled_dag_error_event = result.event_to_fire
        event_list = result.event_list

        # If no modules in DAG, return
        if compiled_dag == 0 and compiled_dag_error_event is not None:
            event_list = fire_empty_line_event(event_list)
            event_list = fire_console_event(
                compiled_dag_error_event,
                event_list,
                log_level='error'
            )
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)

        # ------------------------------------------------------------------------------
        # Create prism project

        project_event_manager = base_event_manager.BaseEventManager(
            idx=None,
            total=None,
            name='parsing config files',
            full_tb=self.args.full_tb,
            func=self.create_project
        )
        project_event_manager_output = project_event_manager.manage_events_during_run(
            event_list=event_list,
            project_dir=project_dir,
            profiles_path=profiles_path,
            env="local",
            which=self.args.which
        )
        prism_project = project_event_manager_output.outputs
        prism_project_event_to_fire = project_event_manager_output.event_to_fire
        event_list = project_event_manager_output.event_list
        if prism_project == 0:
            event_list = fire_empty_line_event(event_list)
            event_list = fire_console_event(
                prism_project_event_to_fire,
                event_list,
                log_level='error'
            )
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)

        # ------------------------------------------------------------------------------
        # Create pipeline

        # First, create DAG executor
        threads = prism_project.thread_count
        dag_executor = prism_executor.DagExecutor(
            project_dir,
            compiled_dag,
            self.args.all_upstream,
            threads
        )

        # Create pipeline
        self.globals_namespace = prism.constants.GLOBALS_DICT.copy()

        # Manager for creating pipeline
        pipeline_manager = base_event_manager.BaseEventManager(
            idx=None,
            total=None,
            name='creating pipeline, DAG executor',
            full_tb=self.args.full_tb,
            func=self.create_pipeline
        )
        pipeline_event_manager_output = pipeline_manager.manage_events_during_run(
            event_list=event_list,
            project=prism_project,
            dag_executor=dag_executor,
            pipeline_globals=self.globals_namespace
        )
        pipeline = pipeline_event_manager_output.outputs
        pipeline_event_to_fire = pipeline_event_manager_output.event_to_fire
        event_list = pipeline_event_manager_output.event_list
        if pipeline == 0:
            event_list = fire_empty_line_event(event_list)
            event_list = fire_console_event(
                pipeline_event_to_fire,
                event_list,
                log_level='error'
            )
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)

        # ------------------------------------------------------------------------------
        # Execute pipeline

        event_list = fire_empty_line_event(event_list)

        # Manager for executing pipeline.
        pipeline_exec_manager = base_event_manager.BaseEventManager(
            idx=None,
            total=None,
            name='executing pipeline',
            full_tb=self.args.full_tb,
            func=pipeline.exec
        )

        # We don't fire the actual exec events for this manager, since executor class
        # fired individual exec events for each module. This manager is simply for
        # capturing any non-module related errors in the logger.
        exec_event_manager_output = pipeline_exec_manager.manage_events_during_run(
            fire_exec_events=False,
            event_list=event_list,
            full_tb=self.args.full_tb
        )
        executor_output = exec_event_manager_output.outputs

        # If executor_output is 0, then an error occurred
        if executor_output == 0:

            # Get the error event and event list
            error_event = exec_event_manager_output.event_to_fire
            executor_events = exec_event_manager_output.event_list

            # Fire error event and return
            event_list = fire_empty_line_event(event_list)
            event_list = fire_console_event(error_event, event_list, log_level='error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)

        # Otherwise, check the status of the executor ouput
        else:
            success = executor_output.success
            error_event = executor_output.error_event
            executor_events = executor_output.event_list
            event_list.extend(executor_events)

            # If success = 0, then there was an error in the DagExecutor
            # multiprocessing. This return structure is confusing; we should eventually
            # fix this.
            if success == 0:
                event_list = fire_empty_line_event(event_list)
                event_list = fire_console_event(
                    error_event,
                    event_list,
                    log_level='error'
                )
                event_list = self.fire_tail_event(event_list)
                return prism.cli.base.TaskRunReturnResult(event_list)

        # ------------------------------------------------------------------------------
        # Fire footer events

        event_list = fire_empty_line_event(event_list)
        event_list = fire_console_event(
            prism.logging.TaskSuccessfulEndEvent(),
            event_list,
            0,
            log_level='info'
        )
        event_list = self.fire_tail_event(event_list)

        # Return
        return prism.cli.base.TaskRunReturnResult(event_list)
