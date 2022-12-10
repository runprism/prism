"""
Connect task class definition, called via `prism connect`

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Standard library imports
import os
from pathlib import Path
from typing import List

# Prism-specific imports
import prism.cli.base
import prism.mixins.connect
import prism.exceptions
import prism.constants
import prism.logging
from prism.event_managers.base import BaseEventManager
from prism.logging import Event, fire_console_event, fire_empty_line_event
from prism.infra import project


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
        # Keep track of events
        event_list: List[Event] = []

        # ------------------------------------------------------------------------------
        # Fire header events

        event_list, project_dir = self.fire_header_events(event_list)
        if project_dir is None:
            return prism.cli.base.TaskRunReturnResult(event_list)

        # Change working directory to project directory
        os.chdir(project_dir)

        event_list = fire_empty_line_event(event_list)

        # ------------------------------------------------------------------------------
        # Define profile type

        adapter_type = self.args.type
        if adapter_type is None:
            e = prism.logging.InvalidAdapterType(prism.constants.VALID_ADAPTERS)
            event_list = fire_console_event(e, event_list, 0, log_level='error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)
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

        # Get user-specified variables. These will override any variables in
        # `prism_project.py`.
        context = self.args.vars
        if context is None:
            context = {}
        prism_project = project.PrismProject(
            project_dir,
            context,
            which="connect",
            filename="prism_project.py",
            flag_compiled=False
        )
        profiles_dir = prism_project.get_profiles_dir(prism_project.prism_project_py_str)
        profiles_filepath = Path(profiles_dir) / 'profile.yml'

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
