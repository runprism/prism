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

    def prepare_paths_for_copy(self, project: PrismProject, tmpdir: str):
        """
        Prism projects often rely on more than just their own directory. They can import
        functions / modules or reference configuration files from other directories. We
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
        return requirements

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
