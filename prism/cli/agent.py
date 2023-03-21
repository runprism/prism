"""
AgentRun task class definition, called via `prism agent run <path to agents.yml>`

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Prism-specific imports
from prism.agents.base import Agent
import prism.cli.base
import prism.cli.run
from prism.event_managers import base as base_event_manager
import prism.mixins.agent
import prism.mixins.run
import prism.exceptions
import prism.constants
import prism.logging
from prism.logging import fire_console_event, fire_empty_line_event, Event
from prism.agents.meta import MetaAgent
from prism.agents import meta  # noqa: F401

# Ohter library imports
import importlib
import json
from pathlib import Path
import os
from typing import List


####################
# Class definition #
####################

class AgentTask(
    prism.cli.run.RunTask,
    prism.mixins.run.RunMixin,
    prism.mixins.agent.AgentMixin
):
    """
    This class allows users to create / run their projects on agents. Agents are simply
    specific computing environments (e.g., Docker containers, EC2 instances,, EMR,
    etc.). This task is called via `prism agent <cmd> -f <path to YAML configuration
    file>`
    """

    def load_agent_conf(self,
        agent_yml_path: Path,
        agent_filename: str,
    ):
        """
        Load the agent configuration from the agent YAML

        args:
            agent_yml_path: Path to agent YAML configuration
        returns:

        """
        agent_yml = self.load_agent_yml(agent_yml_path)
        agent_conf = self.is_valid_agent_yml(agent_filename, agent_yml)
        return agent_conf

    def load_and_run_agent(self,
        args,
        agent_yml_path,
        agent_filename,
        prism_project,
    ):
        """
        Create the agent instance
        """
        # Load the agent configuration
        agent_conf = self.load_agent_conf(
            agent_yml_path,
            agent_filename
        )

        # Create agent instance
        agent_type = agent_conf["type"]
        if agent_type == "docker":
            agent_import = importlib.import_module(
                f'prism.agents.{agent_type}_agent'
            )
            globals()[f"{agent_type}_agent"] = agent_import
        else:
            agent_import = importlib.import_module(
                f'prism.agents.{agent_type}'
            )
            globals()[agent_type] = agent_import
        agent: Agent = MetaAgent.get_agent(agent_type)(
            args,
            agent_yml_path.parent,
            agent_yml_path.name,
            agent_conf,
            prism_project,
        )

        # Run
        if args.which == "agent-apply":
            agent.apply()
        elif args.which == "agent-run":
            agent.run()
        else:
            agent.apply()
            agent.run()

        return

    def run(self) -> prism.cli.base.TaskRunReturnResult:
        """
        Create and/or run the project on an agent.
        """
        # Event list
        event_list: List[Event] = []

        # Separator event
        event_list = fire_console_event(
            prism.logging.SeparatorEvent(),
            event_list
        )

        # Get project directory
        self.project_dir = prism.cli.base.get_project_dir()
        os.chdir(str(self.project_dir))

        # Grab agent file
        agent_yml_path = Path(self.args.file)
        agent_yml_filename = agent_yml_path.name

        # Unlike other tasks, don't fire header events. These will be fired by the agent
        # itself. Instead, just print out a log message indicating that the agent is
        # being created.
        if self.args.which in ["agent-apply", "agent-build"]:
            event_list = fire_console_event(
                prism.logging.CreatingAgentEvent(),
                event_list
            )

        # ------------------------------------------------------------------------------
        # Define PrismProject

        # Get user-specified variables. These will override any variables in
        # `prism_project.py`.
        user_context = json.loads(self.args.context)
        if user_context == {}:
            user_context = self.args.vars
        if user_context is None:
            user_context = {}

        project_event_manager = base_event_manager.BaseEventManager(
            idx=None,
            total=None,
            name='parsing prism_project.py',
            full_tb=self.args.full_tb,
            func=self.create_project
        )
        project_event_manager_output = project_event_manager.manage_events_during_run(
            fire_exec_events=False,
            event_list=event_list,
            project_dir=self.project_dir,
            user_context=user_context,
            which=self.args.which,
            filename="prism_project.py"
        )
        self.prism_project = project_event_manager_output.outputs
        prism_project_event_to_fire = project_event_manager_output.event_to_fire
        event_list = project_event_manager_output.event_list

        # Log an error if one occurs
        if self.prism_project == 0:
            event_list = fire_console_event(
                prism_project_event_to_fire,
                event_list,
                log_level='error'
            )
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list, True)

        # ------------------------------------------------------------------------------
        # Create / execute the agent

        event_list = fire_console_event(
            prism.logging.StreamingLogsStartEvent(),
            event_list
        )
        event_list = fire_empty_line_event(event_list)

        agent_event_manager = base_event_manager.BaseEventManager(
            idx=None,
            total=None,
            name='creating local agent',
            full_tb=self.args.full_tb,
            func=self.load_and_run_agent
        )
        agent_event_manager_output = agent_event_manager.manage_events_during_run(
            fire_exec_events=False,
            event_list=event_list,
            args=self.args,
            agent_yml_path=agent_yml_path,
            agent_filename=agent_yml_filename,
            prism_project=self.prism_project,
        )
        agent_output = agent_event_manager_output.outputs
        agent_event_to_file = agent_event_manager_output.event_to_fire
        event_list = agent_event_manager_output.event_list

        # Log an error if one occurs
        if agent_output == 0:
            event_list = fire_console_event(
                agent_event_to_file,
                event_list,
                log_level='error'
            )
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list, True)

        # Fire tail events
        event_list = fire_empty_line_event()
        event_list = fire_console_event(
            prism.logging.StreamingLogsEndEvent(),
            event_list
        )
        event_list = fire_console_event(
            prism.logging.SeparatorEvent(),
            event_list
        )

        return prism.cli.base.TaskRunReturnResult(event_list, False)
