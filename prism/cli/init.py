"""
Init task class definition, called via `prism init`

Table of Contents
- Imports
- Constants
- Class definition
"""


###########
# Imports #
###########

# Standard library imports
import os
import shutil
import click
from pathlib import Path
from typing import List

# Prism-specific imports
import prism.cli.base
import prism.exceptions
import prism.constants
import prism.logging
from prism.logging import Event, fire_console_event, fire_empty_line_event
from prism.templates.starter_project import STARTER_PROJECT_TEMPLATE_DIR
from prism.templates.minimal_project import MINIMAL_PROJECT_TEMPLATE_DIR


#############
# Constants #
#############

TASK_COMPLETE_MSG = """
      ______
   ____  __ \_____(_)________ _______
 _____  /_/ / ___/ / ___/ __ `__ \ ____
____ / ____/ /  / (__  ) / / / / / _____
 ___/_/   /_/  /_/____/_/ /_/ /_/  ___

Welcome to Prism, the easiest way to create clean, modular data pipelines
using Python!

To get started, navigate to your newly created project "{project_name}" and try
running the following commands:
    prism compile
    prism run

Consult the documentation here for more information on how to get started.
    {docs_url}

Happy building!"""


####################
# Class definition #
####################

class InitTask(prism.cli.base.BaseTask):
    """
    Initialize a Prism project
    """

    def create_starter_project_from_template(self,
        template_dir: str,
        project_dir: Path,
        project_name: str
    ):
        """
        Copy the starter project structure into {project_dir}

        args:
            project_dir: directory for the newly initialized project
        returns:
            None
        """

        # Copy the starter project template into the project directory
        shutil.copytree(
            template_dir,
            project_dir,
            ignore=shutil.ignore_patterns(*prism.constants.IGNORE_FILES)
        )

        # Within the starter project, change the name of the project in globals.yml to
        # match the inputted project name.
        project_yml_path = project_dir / 'prism_project.py'

        # Initialize an empty list of the new lines
        new_lines = []
        with open(project_yml_path, "r+") as f:
            lines = f.readlines()
        f.close()
        for line in lines:
            if line != "@name: ...\n":
                new_lines.append(line)
            elif line == "@name: ...\n":
                new_lines.append(f"@name: {project_name}" + "\n")

        # Write new file
        with open(project_yml_path, "w") as f:
            f.writelines(new_lines)
            f.seek(0)
            f.close()

    def run(self) -> prism.cli.base.TaskRunReturnResult:
        """
        Run the init task
        """

        # Keep track of events
        event_list: List[Event] = []

        # Header events
        event_list = fire_console_event(
            prism.logging.SeparatorEvent(),
            event_list,
            0,
            log_level='info'
        )
        event_list = fire_console_event(
            prism.logging.TaskRunEvent(prism.constants.VERSION),
            event_list,
            log_level='info'
        )
        event_list = fire_empty_line_event(event_list)

        # If the project name wasn't provided by the user, prompt them
        project_name = self.args.project_name
        if project_name is None:
            project_name = click.prompt("What is the desired project name?")
            event_list = fire_empty_line_event(event_list)

        # If the project_name already exists witin the working directory, throw an error
        wkdir = Path.cwd()
        project_dir = wkdir / project_name
        if project_dir.is_dir():
            e = prism.logging.ProjectAlreadyExistsEvent(str(project_dir))
            event_list = fire_console_event(e, event_list, 0, log_level='error')
            event_list = fire_console_event(
                prism.logging.SeparatorEvent(),
                event_list,
                0,
                log_level='info'
            )
            return prism.cli.base.TaskRunReturnResult(event_list)

        # Define template to copy
        if self.args.minimal:
            template_dir = MINIMAL_PROJECT_TEMPLATE_DIR
        else:
            template_dir = STARTER_PROJECT_TEMPLATE_DIR

        # Copy starter project into project directory
        event_list = fire_console_event(
            prism.logging.CreatingProjectDirEvent(),
            event_list,
            log_level='info'
        )
        self.create_starter_project_from_template(
            template_dir,
            project_dir,
            project_name
        )

        # Init task successful
        event_list = fire_empty_line_event(event_list)
        event_list = fire_console_event(
            prism.logging.InitSuccessfulEvent(
                msg=TASK_COMPLETE_MSG.format(
                    project_name=project_name,
                    docs_url='docs.runprism.com'
                )
            ),
            event_list,
            0,
            log_level='info'
        )
        event_list = fire_console_event(
            prism.logging.SeparatorEvent(),
            event_list,
            0,
            log_level='info'
        )

        # Change working directory to the project directory
        os.chdir(project_dir)

        # Return event list
        return prism.cli.base.TaskRunReturnResult(event_list)
