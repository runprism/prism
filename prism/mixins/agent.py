"""
Mixin class for AgentRun task

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Standard library imports
from pathlib import Path
from typing import Any, Dict

# Prism-specific imports
import prism.exceptions
from prism.parsers.yml_parser import YamlParser


####################
# Class definition #
####################

class AgentMixin():
    """
    Mixin class containing functions used to run projects on agents.
    """

    def load_agent_yml(self,
        agent_yml_path: Path
    ) -> Dict[str, Any]:
        """
        Load agents.yml file and store in a dictionary

        args:
            agent_yml_path: Path to agents YAML file
        returns:
            agents YAML file as a dictionary
        """
        parser = YamlParser(agent_yml_path)
        agent_yml = parser.parse()
        return agent_yml

    def is_valid_agent_yml(self,
        agent_filename: str,
        agent_yml: Dict[str, Any]
    ):
        """
        Determine if agents YML file is valid.

        args:
            agent_filename: filename of agents YML
            agent_name: name of desired agent
            agent_yml: agents YML file as a Dictionary
        raises:
            prism.exceptions.InvalidAgentsYmlException if the agents.yml file is not
            properly structured.
        returns:
            True if the agents YML file is properly structured
        """
        # The agent YML has one top-level key: `agent`
        keys = list(agent_yml.keys())
        if len(keys) != 1:
            raise prism.exceptions.InvalidAgentsYmlException(
                message=f"more than one top-level key in `{agent_filename}`"
            )
        if keys[0] != "agent":
            raise prism.exceptions.InvalidAgentsYmlException(
                message=f"invalid top-level key in `{agent_filename}`... should be `agents`"  # noqa: E501
            )

        # Configuration
        agent_conf = agent_yml["agent"]

        # If the configuration doesn't have a `type` parameter, throw an error
        if "type" not in agent_conf.keys():
            raise prism.exceptions.InvalidAgentsConfException(
                message=f"`type` not in agent `{agent_filename}` configuration"
            )

        # If no exception has been raised, then return the agent configuration
        return agent_conf
