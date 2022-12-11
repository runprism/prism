"""
Connect task class definition, called via `prism connect`

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Prism-specific imports
import prism.cli.base
import prism.mixins.connect
import prism.exceptions
import prism.constants
import prism.logging
from prism.event_managers.base import BaseEventManager
from prism.logging import fire_console_event, fire_empty_line_event


####################
# Class definition #
####################

class ConnectTask(prism.cli.base.BaseTask, prism.mixins.connect.ConnectMixin):
    """
    Class for connecting a prism project to an external data warehouse (e.g.,
    snowflake), a big-data processing system (e.g., PySpark), and/or a dbt project
    """

    def run(self) -> prism.cli.base.TaskRunReturnResult:
        """
        Execute connect task
        """
        # ------------------------------------------------------------------------------
        # Fire header events, get prism project
        
        task_return_result = super().run()
        if task_return_result.has_error:
            return task_return_result
        event_list = task_return_result.event_list
        event_list = fire_empty_line_event(event_list)

        # ------------------------------------------------------------------------------
        # Define profile type

        adapter_type = self.args.type

        # If adapter type is None, throw an error
        if adapter_type is None:
            e = prism.logging.InvalidAdapterType(prism.constants.VALID_ADAPTERS)
            event_list = fire_console_event(e, event_list, 0, log_level='error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)

        # If adapter type isn't valid, then throw an error
        elif adapter_type not in prism.constants.VALID_ADAPTERS:
            e = prism.logging.InvalidAdapterType(
                prism.constants.VALID_ADAPTERS,
                adapter_type
            )
            event_list = fire_console_event(e, event_list, 0, log_level='error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)

        # Fire events
        event_list = fire_console_event(
            prism.logging.SettingUpProfileEvent(),
            event_list,
            log_level='info'
        )

        # ------------------------------------------------------------------------------
        # Get profiles dir

        profiles_filepath = self.prism_project.profiles_dir / 'profile.yml'

        # ------------------------------------------------------------------------------
        # Create connection

        # Create a event manager for the connection setup
        connection_event_manager = BaseEventManager(
            idx=None,
            total=None,
            name='connection setup',
            full_tb=self.args.full_tb,
            func=self.create_connection
        )
        event_manager_results = connection_event_manager.manage_events_during_run(
            event_list=event_list,
            fire_exec_events=False,
            profile_type=adapter_type,
            profiles_filepath=profiles_filepath
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
            prism.logging.TaskSuccessfulEndEvent(),
            event_list,
            0,
            log_level='info'
        )
        event_list = self.fire_tail_event(event_list)
        return prism.cli.base.TaskRunReturnResult(event_list)
