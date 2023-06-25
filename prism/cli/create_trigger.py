"""
Class associated with `prism create trigger` CLI command.

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Prism-specific imports
import prism.cli.base
import prism.mixins.create_trigger
import prism.exceptions
import prism.constants
import prism.prism_logging
from prism.event_managers.base import BaseEventManager
from prism.prism_logging import fire_console_event, fire_empty_line_event


####################
# Class definition #
####################

class CreateTriggerTask(
    prism.cli.base.BaseTask,
    prism.mixins.create_trigger.CreateTriggersMixin
):
    """
    Class for generating a triggers.yml file. This is accessed via the `prism create
    trigger`.
    """

    def run(self) -> prism.cli.base.TaskRunReturnResult:
        """
        Create a `triggers.yml` file according to the user's specifications
        """

        # ------------------------------------------------------------------------------
        # Fire header events, get prism project

        task_return_result: prism.cli.base.TaskRunReturnResult = super().run()
        if task_return_result.has_error:
            return task_return_result
        event_list = task_return_result.event_list
        event_list = fire_empty_line_event(event_list)

        # ------------------------------------------------------------------------------
        # Define trigger type

        trigger_type = self.args.type

        # If adapter type is None, throw an error
        if trigger_type is None:
            e = prism.prism_logging.InvalidType(
                "trigger",
                prism.constants.VALID_TRIGGER_TYPES
            )
            event_list = fire_console_event(e, event_list, 0, log_level='error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)

        # If adapter type isn't valid, then throw an error
        elif trigger_type not in prism.constants.VALID_TRIGGER_TYPES:
            e = prism.prism_logging.InvalidType(
                "trigger",
                prism.constants.VALID_TRIGGER_TYPES,
                trigger_type
            )
            event_list = fire_console_event(e, event_list, 0, log_level='error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)

        # Fire events
        event_list = fire_console_event(
            prism.prism_logging.CreatingTriggersEvent(),
            event_list,
            log_level='info'
        )

        # ------------------------------------------------------------------------------
        # Get triggers dir

        if self.prism_project.triggers_yml_path is None:
            triggers_filepath = self.prism_project.project_dir / 'triggers.yml'
        else:
            triggers_filepath = self.prism_project.triggers_yml_path

        # ------------------------------------------------------------------------------
        # Create trigger

        # Create a event manager for the trigger setup
        trigger_event_manager = BaseEventManager(
            idx=None,
            total=None,
            name='triggers setup',
            full_tb=self.args.full_tb,
            func=self.create_trigger
        )
        event_manager_results = trigger_event_manager.manage_events_during_run(
            event_list=event_list,
            fire_exec_events=False,
            trigger_type=trigger_type,
            triggers_filepath=triggers_filepath
        )
        success = event_manager_results.outputs
        event_to_fire = event_manager_results.event_to_fire
        event_list = event_manager_results.event_list
        if success == 0:
            event_list = fire_empty_line_event(event_list)
            event_list = fire_console_event(
                event_to_fire,
                event_list,
                log_level='error'
            )
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)

        # Fire footer events
        event_list = fire_empty_line_event(event_list)
        event_list = fire_console_event(
            prism.prism_logging.TaskSuccessfulEndEvent(),
            event_list,
            0,
            log_level='info'
        )
        event_list = self.fire_tail_event(event_list)
        return prism.cli.base.TaskRunReturnResult(event_list)
