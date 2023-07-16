"""
Base Agent class. This class allows users to run projects in containers (e.g., Docker)
or virtual machine instances (e.g., EC2, EMR).
"""


###########
# Imports #
###########

# Prism imports
import prism.exceptions
from prism.infra.project import PrismProject
from .meta import MetaAgent

# Standard library imports
import argparse
from pathlib import Path
from typing import Any, Dict


####################
# Class definition #
####################

class Agent(metaclass=MetaAgent):
    """
    The `agents.yml` file will be formatted as follows:

    agents:
      <agent name here>:
        type: docker
        ...
    """

    def __init__(self,
        args: argparse.Namespace,
        agent_dir: Path,
        agent_filename: str,
        agent_conf: Dict[str, Any],
        project: PrismProject,
        mode: str = "prod"
    ):
        """
        Create agent

        args:
            args: user arguments
            agent_dir: directory of agent YAML configuration file
            agent_filename: filename of agent YAML configuration file
            agent_conf: agent configuration
            project: PrismProject instance
        """
        self.args = args

        # Agents file location
        self.agent_dir = agent_dir
        self.agent_filename = agent_filename
        self.agents_yml_path = self.agent_dir / self.agent_filename

        # Use the filename as the agent name
        self.agent_name = self.agent_filename.split(".")[0]

        # Prism Project. We unfortunately need to parse prism_project.py before running
        # our code on our agent, because the prism_project.py tells us what paths we
        # will need to copy over (e.g., paths in SYS_PATH_CONF, profile paths, etc.)
        self.project = project

        # Mode
        self.mode = mode

        # We already confirm that the agent YAML is correctly structured
        self.agent_conf = agent_conf
        self.is_valid_conf(self.agent_conf)

    def is_valid_conf(self, agent_conf: Dict[str, Any]):
        """
        Determine if agent configuration is valid. This should be implemented by all
        child classes of Agent.
        """
        raise prism.exceptions.NotImplementedException(
            message=f"`is_valid_conf` not implemented for agent {self.__class__.__name__}"  # noqa: E501
        )

    def check_conf_keys(self,
        agent_conf: Dict[str, Any],
        required_keys: Dict[str, Any],
        optional_keys: Dict[str, Any]
    ):
        """
        Check the presence of required and/or optional keys in the agent's configuration

        args:
            agent_conf: agent configuration
            required_keys: the agent's required keys
            optional_keys: the agent's optional keys
        returns:
            True if the configuration has all required keys and the required keys are
            the correct type. False otherwise
        """
        # Check required keys
        for _key, _type in required_keys.items():
            if _key not in list(agent_conf.keys()):
                raise prism.exceptions.InvalidAgentsConfException(
                    message=f"`{_key}` not in agent `{self.agent_name}` configuration"
                )

            if not isinstance(agent_conf[_key], _type):
                raise prism.exceptions.InvalidAgentsConfException(
                    message=f"`{_key}` is not the correct type"
                )

        # Check optional keys, if they exist
        for _key, _type in optional_keys.items():  # type: ignore
            if _key in list(agent_conf.keys()):
                if (
                    agent_conf[_key] is not None
                    and not isinstance(agent_conf[_key], _type)  # noqa: W503
                ):
                    raise prism.exceptions.InvalidAgentsConfException(
                        message=f"`{_key}` is not the correct type"
                    )

        # If no exception has been raised, return True
        return True

    def prepare_paths_for_copy(self, project: PrismProject, tmpdir: str):
        """
        Prism projects often rely on more than just their own directory. They can import
        functions / tasks or reference configuration files from other directories. We
        need to copy all relevant directories into our agent, and we need to ensure that
        these paths have the same relative location to the project directory.

        args:
            project: Prism project
        returns:
            commands to copy directories into agent's computing environment
        """
        raise prism.exceptions.NotImplementedException(
            message=f"`identify_copy_paths` not implemented for agent {self.__class__.__name__}"  # noqa: E501
        )

    def parse_requirements(self, agent_conf: Dict[str, Any]):
        """
        Get the requirements.txt path and construct the pip install statement.

        args:
            agent_conf: agent configuration as dictionary
        returns:
            `pip install` command for requirements
        """
        # We already know that the agent configuration is valid. Therefore, it must have
        # a requirements key.
        requirements = agent_conf["requirements"]

        # The `requirements.txt` path should always be specified relative to the
        # directory of the agent YAML file.
        absolute_requirements_path = Path(self.agent_dir / requirements).resolve()

        # Check if this file exists
        if not absolute_requirements_path.is_file():
            raise prism.exceptions.FileNotFoundException(
                f"no file found at {absolute_requirements_path}"
            )
        return absolute_requirements_path

    def construct_command(self):
        """
        Construct Prism CLI command from arguments
        """

        # Construct command
        full_tb = self.args.full_tb
        log_level = self.args.log_level
        vars = self.args.vars
        context = self.args.context
        tasks = self.args.tasks
        all_upstream = self.args.all_upstream
        all_downstream = self.args.all_downstream

        # Namespace to string conversion
        full_tb_cmd = "" if not full_tb else "--full-tb"
        log_level_cmd = "" if log_level == "info" else f"--log-level {log_level}"
        vars_cmd = "" if vars is None else " ".join([
            f"{k}={v}" for k, v in vars.items()
        ])
        context_cmd = "" if context == '{}' else f"--context '{context}'"
        tasks_cmd = "" if tasks is None else " " .join([
            f"--task {m}" for m in tasks
        ])
        all_upstream_cmd = "" if not all_upstream else "--all-upstream"
        all_downstream_cmd = "" if not all_downstream else "--all-downstream"

        # Full command
        full_cmd = f"prism run {full_tb_cmd} {log_level_cmd} {vars_cmd} {context_cmd} {tasks_cmd} {all_upstream_cmd} {all_downstream_cmd}"  # noqa: E501
        return full_cmd

    def parse_environment_variables(self, agent_conf: Dict[str, Any]) -> Dict[str, str]:
        raise prism.exceptions.NotImplementedException(
            message=f"`parse_environment_variables` not implemented for agent {self.__class__.__name__}"  # noqa: E501
        )

    def apply(self):
        raise prism.exceptions.NotImplementedException(
            message=f"`apply` not implemented for agent {self.__class__.__name__}"  # noqa: E501
        )

    def run(self):
        raise prism.exceptions.NotImplementedException(
            message=f"`run` not implemented for agent {self.__class__.__name__}"  # noqa: E501
        )

    def delete(self):
        raise prism.exceptions.NotImplementedException(
            message=f"`delete` not implemented for agent {self.__class__.__name__}"  # noqa: E501
        )
