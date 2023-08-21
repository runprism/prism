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
import prism.prism_logging
from prism.prism_logging import fire_console_event, fire_empty_line_event
from prism.event_managers import base as base_event_manager
from prism.infra import executor as prism_executor
from prism.triggers import TriggerManager

# Ohter library imports
import json
from typing import List, Optional


####################
# Class definition #
####################

class RunTask(prism.cli.compile.CompileTask, prism.mixins.run.RunMixin):
    """
    Class for defining the "run" task
    """

    def fire_error_events(self,
        event_list: List[prism.prism_logging.Event],
        error_event: Optional[prism.prism_logging.Event],
        trigger_manager: TriggerManager
    ):
        """
        Fire error events, including triggers
        """
        # Fire console event
        event_list = fire_console_event(
            error_event,
            event_list,
            log_level='error',
        )

        # Fire triggers
        cb_output = trigger_manager.exec(
            'on_failure',
            self.args.full_tb,
            event_list,
            self.run_context,
        )
        event_list = self.fire_tail_event(cb_output.event_list)
        return event_list

    def run(self) -> prism.cli.base.TaskRunReturnResult:
        """
        Execute run task. Function is organized as follows:

        - Fire header events, get prism project
        - Compile DAG
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
        # Run the sys.path engine

        sys_path_engine = self.prism_project.sys_path_engine
        self.run_context = sys_path_engine.modify_sys_path(
            self.prism_project.sys_path_config
        )

        # ------------------------------------------------------------------------------
        # Prepare triggers

        triggers_yml_path = self.prism_project.triggers_yml_path
        trigger_manager = TriggerManager(
            triggers_yml_path,
            self.prism_project,
        )

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

        # If no tasks in DAG, return
        if compiled_dag == 0 and compiled_dag_error_event is not None:
            event_list = self.fire_error_events(
                event_list,
                compiled_dag_error_event,
                trigger_manager
            )
            self.run_context = self.prism_project.cleanup(self.run_context)
            return prism.cli.base.TaskRunReturnResult(event_list, True)

        # ------------------------------------------------------------------------------
        # Create pipeline

        # Get user-specified variables. These will override any variables in
        # `prism_project.py`.
        user_context = json.loads(self.args.context)
        if user_context == {}:
            user_context = self.args.vars
        if user_context is None:
            user_context = {}

        # First, create DAG executor
        threads = self.prism_project.thread_count
        dag_executor = prism_executor.DagExecutor(
            self.project_dir,
            compiled_dag,
            self.args.all_upstream,
            self.args.all_downstream,
            threads,
            self.args.full_refresh,
            user_context
        )

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
            fire_exec_events=True,
            fire_empty_line_events=True,
            project=self.prism_project,
            dag_executor=dag_executor,
            run_context=self.run_context
        )
        pipeline = pipeline_event_manager_output.outputs
        pipeline_event_to_fire = pipeline_event_manager_output.event_to_fire
        event_list = pipeline_event_manager_output.event_list
        if pipeline == 0:
            event_list = self.fire_error_events(
                event_list,
                pipeline_event_to_fire,
                trigger_manager
            )
            self.run_context = self.prism_project.cleanup(self.run_context)
            return prism.cli.base.TaskRunReturnResult(event_list, True)

        # ------------------------------------------------------------------------------
        # Execute pipeline

        event_list = fire_empty_line_event(event_list)
        event_list = fire_console_event(
            prism.prism_logging.TasksHeaderEvent(msg=self.prism_project.slug),
            event_list
        )

        # Manager for executing pipeline.
        pipeline_exec_manager = base_event_manager.BaseEventManager(
            idx=None,
            total=None,
            name='executing pipeline',
            full_tb=self.args.full_tb,
            func=pipeline.exec
        )

        # We don't fire the actual exec events for this manager, since executor class
        # fired individual exec events for each task. This manager is simply for
        # capturing any non-task related errors in the logger.
        exec_event_manager_output = pipeline_exec_manager.manage_events_during_run(
            fire_exec_events=False,
            fire_empty_line_events=False,
            event_list=event_list,
            full_tb=self.args.full_tb
        )
        executor_output = exec_event_manager_output.outputs

        # If executor_output is 0, then an error occurred
        if executor_output == 0:
            event_list = self.fire_error_events(
                exec_event_manager_output.event_list,
                exec_event_manager_output.event_to_fire,
                trigger_manager
            )
            pipeline.run_context = self.prism_project.cleanup(pipeline.run_context)
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
                event_list = self.fire_error_events(
                    event_list,
                    error_event,
                    trigger_manager
                )
                pipeline.run_context = self.prism_project.cleanup(pipeline.run_context)
                return prism.cli.base.TaskRunReturnResult(event_list, True)

        # ------------------------------------------------------------------------------
        # Fire footer events

        event_list = fire_empty_line_event(event_list)
        cb_return_result = trigger_manager.exec(
            'on_success',
            self.args.full_tb,
            event_list,
            pipeline.run_context,
        )
        event_list = cb_return_result.event_list

        # If the triggers do not have an error, then fire a TaskSuccessfulEndEvent
        if not cb_return_result.has_error:

            # Only fire an empty line if we had triggers to execute
            if len(trigger_manager.on_success_triggers) > 0:
                event_list = fire_empty_line_event(event_list)

            # Task is successful
            event_list = fire_console_event(
                prism.prism_logging.TaskSuccessfulEndEvent(),
                event_list,
                0,
                log_level='info'
            )
            event_list = self.fire_tail_event(event_list)

        # Otherwise, just fire the tail event
        else:
            event_list = self.fire_tail_event(event_list)

        # Undo any sys.path changes
        pipeline.run_context = self.prism_project.cleanup(pipeline.run_context)

        # Return
        return prism.cli.base.TaskRunReturnResult(event_list)
