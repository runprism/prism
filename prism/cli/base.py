"""
Base task class definition

Table of Contents
- Imports
- Functions / utils
- Class definition
"""


#############
## Imports ##
#############

# Standard library imports
import os
from pathlib import Path
from typing import List, Tuple, Union

# Prism-specific imports
import prism.exceptions
import prism.logging
from prism.logging import fire_console_event, fire_empty_line_event


#######################
## Functions / utils ##
#######################

# Functions for identifying project directory and charging the current working directory accordingly. These will not be
# used for the init task, but they will be used for others (e.g., the run task).
def get_project_dir():
    """
    Get the project directory with a prism_project.py file by iterating through current working directory and parents
    until the prism_project.py directory is found.
    
    args:
        None
    returns:
        project_dir: nearest project directory with prism_project.py file
    """
    
    # Otherwise, check parent directories for prism_project.py file
    root_path = Path(os.sep).resolve()
    cwd = Path.cwd()

    while cwd!=root_path:
        project_file = cwd / 'prism_project.py'
        if project_file.is_file():
            if not Path(cwd / 'modules').is_dir():
                raise prism.exceptions.ModulesDirNotFoundException(
                    'modules directory not found in project directory or any of its parents'
                )
            return cwd
        else:
            cwd = cwd.parent

    raise prism.exceptions.ProjectPyNotFoundException(
        'prism_project.py file not found in current directory or any of its parents'
    )


def move_to_nearest_project_dir():
    """
    Set working directory to nearest project directory

    args:
        args: user arguments
    returns:
        None
    """
    project_dir = get_project_dir()
    os.chdir(project_dir)


######################
## Class definition ##
######################

class TaskRunReturnResult:
    """
    Return result for tasks
    """

    def __init__(self, event_list: List[prism.logging.Event]):
        self.event_list = event_list
    

    def get_results(self):
        return ' | '.join([x.__str__() for x in self.event_list])
        

class BaseTask:
    """
    Interface for defining common behavior of tasks
    """

    def __init__(self, args):
        self.args = args


    @classmethod
    def task_from_args(cls, args):
        return cls(args)


    def fire_header_events(self,
        event_list: List[prism.logging.Event] = []
    ) -> Tuple[List[prism.logging.Event], Union[Path, None]]:
        """
        Fire header events that should be displayed at the beginning of all tasks (except the init task)
        """
        event_list = fire_console_event(prism.logging.SeparatorEvent(), event_list, 1)
        event_list = fire_console_event(prism.logging.TaskRunEvent(version=prism.constants.VERSION), event_list, 0)

        try:
            project_dir = get_project_dir()
            event_list = fire_console_event(prism.logging.CurrentProjectDirEvent(project_dir), event_list, 0)
            return event_list, project_dir
        except prism.exceptions.ProjectPyNotFoundException as err:
            event_list = fire_empty_line_event(event_list)
            e = prism.logging.ProjectPyNotFoundEvent(err)
            event_list = fire_console_event(e, event_list)
            event_list = fire_console_event(prism.logging.SeparatorEvent(), event_list, 0)
            return event_list, None


    def run(self):
        """
        Execute the task
        """
        print('Hello world!')


# EOF