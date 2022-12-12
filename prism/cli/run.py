"""
Run task class definition, called via `prism run`

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Prism-specific imports
import prism.cli.base
import prism.cli.compile
import prism.mixins.run
import prism.exceptions
import prism.constants
import prism.logging
from prism.logging import fire_console_event, fire_empty_line_event
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

        # ------------------------------------------------------------------------------
        # Fire header events, get prism project

        task_return_result: prism.cli.base.TaskRunReturnResult = super(prism.cli.compile.CompileTask, self).run()  # noqa: E501
        if task_return_result.has_error or self.project_dir is None:
            return task_return_result
        event_list = task_return_result.event_list

        # ------------------------------------------------------------------------------
        # Compile DAG

        result = super().run_for_subclass(
            self.args,
            self.project_dir,
            event_list,
            self.prism_project,
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
            return prism.cli.base.TaskRunReturnResult(event_list, True)

        # ------------------------------------------------------------------------------
        # Create pipeline

        # Get user-specified variables. These will override any variables in
        # `prism_project.py`.
        user_context = self.args.vars
        if user_context is None:
            user_context = {}

        # First, create DAG executor
        threads = self.prism_project.thread_count
        dag_executor = prism_executor.DagExecutor(
            self.project_dir,
            compiled_dag,
            self.args.all_upstream,
            threads,
            user_context
        )

        # Create pipeline
        self.run_context = self.prism_project.run_context

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
            project=self.prism_project,
            dag_executor=dag_executor,
            run_context=self.run_context
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
            return prism.cli.base.TaskRunReturnResult(event_list, True)

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
            return prism.cli.base.TaskRunReturnResult(event_list, True)

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
                return prism.cli.base.TaskRunReturnResult(event_list, True)

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
