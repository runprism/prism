"""
Mixin class for CreateAgent task

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Standard library imports
import shutil
from pathlib import Path

# Prism-specific imports
import prism.cli.base
import prism.cli.compile
import prism.exceptions
import prism.constants
import prism.logging
from prism.templates.agents import AGENTS_TEMPLATE_DIR as agents_template_dir


####################
# Class definition #
####################

class CreateAgentMixin():
    """
    Mixin for CreateTrigger task
    """

    def create_directory(self,
        path: str
    ):
        """
        Create a directory at `path` if it doesn't exist.

        args:
            path: path to create a directory
        returns:
            None
        """
        if not Path(path).is_dir():
            p = Path(path)
            p.mkdir(parents=True, exist_ok=True)

    def create_agent_from_template(self,
        type: str,
        agent_filepath: Path
    ):
        f"""
        Create a agent YAML file using the template.

        args:
            type: trigger type; one of {prism.constants.VALID_AGENTS}
            agent_filepath: location to keep the the agent YAML configuration file
        returns:
            None
        """

        # Make the directory, if it doesn't exist
        self.create_directory(agent_filepath.parent)

        # We only ever call this function after confirming that the agent YAML file does
        # not exist and that the type is valid.
        agent_template_path = Path(agents_template_dir) / f'{type}.yml'
        shutil.copyfile(agent_template_path, agent_filepath)

    def create_agent(self,
        agent_type: str,
        agent_filepath: Path
    ):
        """
        Create a trigger for the inputted `agent_type`

        args:
            agent_type: trigger type (either `function` or `prism_project`)
            agent_filepath: path to agent YAML
        """
        # If the profile doesn't exist, then create it
        if not agent_filepath.is_file():
            self.create_agent_from_template(agent_type, agent_filepath)
            return
        else:
            raise prism.exceptions.AgentAlreadyExistsException(
                f"agent already exists at {str(agent_filepath)}"
            )
