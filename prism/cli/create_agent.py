"""
Class associated with `prism create agent` CLI command.

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Prism-specific imports
import prism.cli.base
import prism.mixins.create_agent
import prism.exceptions
import prism.constants
import prism.prism_logging
from prism.event_managers.base import BaseEventManager
from prism.prism_logging import fire_console_event, fire_empty_line_event


####################
# Class definition #
####################

class CreateAgentTask(
    prism.cli.base.BaseTask,
    prism.mixins.create_agent.CreateAgentMixin
):
    """
    Class for generating a agent YAML file. This is accessed via the `prism create
    agent`.
    """

    def run(self) -> prism.cli.base.TaskRunReturnResult:
        """
        Create a agent YML file according to the user's specifications
        """

        # ------------------------------------------------------------------------------
        # Fire header events, get prism project

        task_return_result: prism.cli.base.TaskRunReturnResult = super().run()
        if task_return_result.has_error:
            return task_return_result
        event_list = task_return_result.event_list
        event_list = fire_empty_line_event(event_list)

        # ------------------------------------------------------------------------------
        # Define agent type

        agent_type = self.args.type

        # If adapter type is None, throw an error
        if agent_type is None:
            e = prism.prism_logging.InvalidType(
                "agent",
                prism.constants.VALID_AGENTS
            )
            event_list = fire_console_event(e, event_list, 0, log_level='error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)

        # If agent type isn't valid, then throw an error
        elif agent_type not in prism.constants.VALID_AGENTS:
            e = prism.prism_logging.InvalidType(
                "agent",
                prism.constants.VALID_AGENTS,
                agent_type
            )
            event_list = fire_console_event(e, event_list, 0, log_level='error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)

        # ------------------------------------------------------------------------------
        # Get agent filepath

        from pathlib import Path
        agent_filepath = Path(self.args.file)
        event_list = fire_console_event(
            prism.prism_logging.CreatingAgentYamlEvent(str(agent_filepath)),
            event_list,
            log_level='info'
        )

        # ------------------------------------------------------------------------------
        # Create agent

        # Create a event manager for the agent setup
        agent_event_manager = BaseEventManager(
            idx=None,
            total=None,
            name='agent setup',
            full_tb=self.args.full_tb,
            func=self.create_agent
        )
        event_manager_results = agent_event_manager.manage_events_during_run(
            event_list=event_list,
            fire_exec_events=False,
            agent_type=agent_type,
            agent_filepath=agent_filepath
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
