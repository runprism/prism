# Standard library imports
import shutil
from pathlib import Path
from typing import Literal, Optional

import click

import prism.constants

# Prism-specific imports
import prism.db.setup
import prism.exceptions
from prism.logging.events import (
    CreatingPrismProjectTemplate,
    InitSuccessfulEvent,
    fire_empty_line_event,
    fire_init_events,
    fire_tail_events,
)
from prism.logging.loggers import console_print, set_up_logger
from prism.templates.starter_project import STARTER_PROJECT_TEMPLATE_DIR

TASK_COMPLETE_MSG = """      ______
   ____  __ \_____(_)________ _______
 _____  /_/ / ___/ / ___/ __ `__ \ ____
____ / ____/ /  / (__  ) / / / / / _____
 ___/_/   /_/  /_/____/_/ /_/ /_/  ___

Welcome to Prism, the easiest way to create clean, modular data pipelines
using Python!

To get started, navigate to your newly created project "{project_name}" and try
running the following commands:
    > python main.py
    > prism run
    > prism graph

Consult the documentation here for more information on how to get started.
    {docs_url}

Happy building!"""


def initialize_project(
    project_name: Optional[str],
    log_level: Literal["info", "warning", "error", "debug", "critical"],
) -> None:
    """
    Initialize a Prism project. The project itself is nothing special — it's just a
    template project to help the user get started.

    args:
        project_name: name for new project
        log_level: log level
    returns:
        None
    """
    set_up_logger(log_level, None)
    fire_init_events()

    # If the project name wasn't provided by the user, prompt them
    if project_name is None:
        project_name = click.prompt("What is the desired project name?")
        fire_empty_line_event()

    # Set up the database
    prism.db.setup.setup()

    # If the project_name already exists witin the working directory, throw an error
    wkdir = Path.cwd()
    project_dir = wkdir / project_name
    if project_dir.is_dir():
        raise prism.exceptions.ProjectAlreadyExistsException(project_dir)

    # Template directory
    template_dir = STARTER_PROJECT_TEMPLATE_DIR
    console_print(CreatingPrismProjectTemplate(project_dir).message())
    shutil.copytree(
        template_dir,
        project_dir,
        ignore=shutil.ignore_patterns(*prism.constants.IGNORE_FILES),
    )
    fire_empty_line_event()
    console_print(
        InitSuccessfulEvent(
            msg=TASK_COMPLETE_MSG.format(
                project_name=project_name, docs_url="docs.runprism.com"
            )
        ).message()
    )
    fire_tail_events()
    return None
