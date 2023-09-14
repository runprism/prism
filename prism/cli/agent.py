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
from prism.infra.project import PrismProject
import prism.prism_logging
from prism.prism_logging import fire_console_event, fire_empty_line_event, Event
from prism.agents.meta import MetaAgent
from prism.agents import meta  # noqa: F401

# Ohter library imports
import importlib
import json
from pathlib import Path
import os
from typing import List, Union


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
        prism_project: PrismProject,
    ):
        """
        Load the agent configuration from the agent YAML

        args:
            agent_yml_path: Path to agent YAML configuration
            agent_filename: agent YAML configuration filename
        returns:
            agent configuration as a dictionary
        """
        agent_yml = self.load_agent_yml(agent_yml_path, prism_project)
        agent_conf = self.is_valid_agent_yml(agent_filename, agent_yml)
        return agent_conf

    def build_agent(self,
        args,
        agent_yml_path: Path,
        agent_filename: str,
        prism_project: PrismProject,
    ) -> Union[int, Agent]:
        """
        Create the agent instance
        """
        # Load the agent configuration
        agent_conf = self.load_agent_conf(
            agent_yml_path,
            agent_filename,
            prism_project
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

        # If we just want to run the agent, we do that in a separate function call.
        if args.which == "agent-run":
            return agent

        elif args.which == "agent-delete":
            agent.delete()
            return agent

        # Otherwise, we either want to build the agent (`prism agent apply`) or build &
        # run the agent (`prism agent build`). We run the agent in a separate function,
        # so focus on just building it for now.
        else:
            returncode = agent.apply()
            if returncode["return_code"] != 0:
                return 0
            else:
                return agent

    def run_agent(self, agent: Agent):
        """
        Thin wrapper around the agent `run` function.
        """
        return agent.run()

    def run(self) -> prism.cli.base.TaskRunReturnResult:
        """
        Create and/or run the project on an agent.
        """
        # Event list
        event_list: List[Event] = []

        # Separator event
        event_list = fire_console_event(
            prism.prism_logging.SeparatorEvent(),
            event_list
        )

        # Get project directory
        self.project_dir = prism.cli.base.get_project_dir()
        os.chdir(str(self.project_dir))

        # Grab agent file
        agent_yml_path = Path(self.args.file)
        agent_yml_filename = agent_yml_path.name

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
        # Delete the agent

        # If the user wants to delete the agent, then execute the delete function and
        # return
        if self.args.which == "agent-delete":
            event_list = fire_console_event(
                prism.prism_logging.DeletingAgentEvent(),
                event_list
            )

        # ------------------------------------------------------------------------------
        # Create the agent

        # Fire a log event indicating that we're creating the agent (we only do this for
        # `agent-apply` and `agent-build`).
        elif self.args.which in ["agent-apply", "agent-build"]:
            event_list = fire_console_event(
                prism.prism_logging.CreatingAgentEvent(),
                event_list
            )

        # The build_agent function will only actually *build* the agent if `agent-apply`
        # or `agent-build` is called. Otherwise, it will just instantiate the Agent
        # object and return it.
        build_manager = base_event_manager.BaseEventManager(
            idx=None,
            total=None,
            name='creating local agent',
            full_tb=self.args.full_tb,
            func=self.build_agent
        )
        build_manager_output = build_manager.manage_events_during_run(
            fire_exec_events=False,
            event_list=event_list,
            args=self.args,
            agent_yml_path=agent_yml_path,
            agent_filename=agent_yml_filename,
            prism_project=self.prism_project,
        )

        agent = build_manager_output.outputs
        build_event_to_fire = build_manager_output.event_to_fire
        event_list = build_manager_output.event_list

        # Log any Python error that we encounter
        if agent == 0:
            event_list = fire_console_event(
                build_event_to_fire,
                event_list,
                log_level='error'
            )
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list, True)

        # If the `apply` script exits with a non-zero return code, then exit
        if isinstance(agent, dict) and agent["return_code"] != 0:
            event_list = fire_console_event(
                prism.prism_logging.SeparatorEvent(),
                event_list
            )
            return prism.cli.base.TaskRunReturnResult(event_list, True)

        # ------------------------------------------------------------------------------
        # Execute the agent

        if self.args.which in ["agent-apply", "agent-build"]:
            event_list = fire_empty_line_event(event_list)

        # Only execute the agent with `agent-run` or `agent-build`
        if self.args.which in ["agent-run", "agent-build"]:
            event_list = fire_console_event(
                prism.prism_logging.StreamingLogsStartEvent(),
                event_list
            )
            event_list = fire_empty_line_event(event_list)

            agent_event_manager = base_event_manager.BaseEventManager(
                idx=None,
                total=None,
                name='creating local agent',
                full_tb=self.args.full_tb,
                func=self.run_agent
            )
            agent_event_manager_output = agent_event_manager.manage_events_during_run(
                fire_exec_events=False,
                event_list=event_list,
                agent=agent
            )
            agent_output = agent_event_manager_output.outputs
            agent_event_to_fire = agent_event_manager_output.event_to_fire
            event_list = agent_event_manager_output.event_list

            # Log any Python error that we encounter
            if agent_output == 0:
                event_list = fire_console_event(
                    agent_event_to_fire,
                    event_list,
                    log_level='error'
                )
                event_list = self.fire_tail_event(event_list)
                return prism.cli.base.TaskRunReturnResult(event_list, True)

            # If the `run` script exits with a non-zero return code, then return
            if isinstance(agent_output, dict) and agent_output["return_code"] != 0:
                event_list = fire_console_event(
                    prism.prism_logging.SeparatorEvent(),
                    event_list
                )
                return prism.cli.base.TaskRunReturnResult(event_list, True)

            # Now, we're done streaming logs
            event_list = fire_empty_line_event()
            event_list = fire_console_event(
                prism.prism_logging.StreamingLogsEndEvent(),
                event_list
            )

        # Tail events
        event_list = fire_console_event(
            prism.prism_logging.SeparatorEvent(),
            event_list
        )

        return prism.cli.base.TaskRunReturnResult(event_list, False)
