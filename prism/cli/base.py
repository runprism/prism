"""
Base task class definition

Table of Contents
- Imports
- Functions / utils
- Class definition
"""


###########
# Imports #
###########

# Standard library imports
import json
import os
from pathlib import Path
from typing import List, Tuple, Union

# Prism-specific imports
from prism.event_managers import base as base_event_manager
import prism.exceptions
import prism.prism_logging
from prism.prism_logging import fire_console_event, fire_empty_line_event, Event
from prism.mixins import base as base_mixins


#####################
# Functions / utils #
#####################

# Functions for identifying project directory and charging the current working directory
# accordingly. These will not be used for the init task, but they will be used for
# others (e.g., the run task).
def get_project_dir():
    """
    Get the project directory with a prism_project.py file by iterating through current
    working directory and parents until the prism_project.py directory is found.

    args:
        None
    returns:
        project_dir: nearest project directory with prism_project.py file
    """

    # Otherwise, check parent directories for prism_project.py file
    root_path = Path(os.sep).resolve()
    cwd = Path.cwd()

    while cwd != root_path:
        project_file = cwd / 'prism_project.py'
        if project_file.is_file():
            if not Path(cwd / 'models').is_dir():
                models_dir_not_found_msg = ' '.join([
                    'models directory not found in project directory or any',
                    'of its parents',
                ])
                raise prism.exceptions.ModelsDirNotFoundException(
                    models_dir_not_found_msg
                )
            return cwd
        else:
            cwd = cwd.parent

    project_py_no_found_msg = ' '.join([
        'prism_project.py file not found in current directory or any',
        'of its parents',
    ])
    raise prism.exceptions.ProjectPyNotFoundException(
        project_py_no_found_msg
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


####################
# Class definition #
####################

class TaskRunReturnResult:
    """
    Return result for tasks
    """

    def __init__(self,
        event_list: List[prism.prism_logging.Event],
        has_error: bool = False
    ):
        self.event_list = event_list
        self.has_error = has_error

    def get_results(self):
        return ' | '.join([x.__str__() for x in self.event_list])


class BaseTask(base_mixins.BaseMixin):
    """
    Interface for defining common behavior of tasks
    """

    def __init__(self, args):
        self.args = args
        prism.prism_logging.set_up_logger(self.args)

    @classmethod
    def task_from_args(cls, args):
        return cls(args)

    def fire_header_events(self,
        event_list: List[prism.prism_logging.Event] = []
    ) -> Tuple[List[prism.prism_logging.Event], Union[Path, None]]:
        """
        Fire header events that should be displayed at the beginning of all tasks
        (except the init task)
        """
        # Empty list for events
        base_event_list: List[Event] = []

        # Fire header events
        event_list = fire_console_event(
            prism.prism_logging.SeparatorEvent(), base_event_list, 1, 'info'
        )
        event_list = fire_console_event(
            prism.prism_logging.TaskRunEvent(version=prism.constants.VERSION),
            event_list,
            0,
            'info'
        )

        # Get project directory
        try:
            project_dir = get_project_dir()
            event_list = fire_console_event(
                prism.prism_logging.CurrentProjectDirEvent(project_dir),
                event_list,
                0,
                'info'
            )
            return event_list, project_dir

        # If project directory not found, fire an event
        except prism.exceptions.ProjectPyNotFoundException as err:
            event_list = fire_empty_line_event(event_list)
            e = prism.prism_logging.ProjectPyNotFoundEvent(err)
            event_list = fire_console_event(e, event_list, log_level='error')
            event_list = self.fire_tail_event(event_list)
            return event_list, None

    def fire_tail_event(self,
        event_list: List[prism.prism_logging.Event] = []
    ) -> List[prism.prism_logging.Event]:
        """
        Fire tail event
        """
        # Fire a separator event to indicate the end of a run. Note, this will
        # only fire if --quietly isn't invoked
        event_list = prism.prism_logging.fire_console_event(
            prism.prism_logging.SeparatorEvent(),
            event_list,
            sleep=0,
            log_level='info'
        )
        return event_list

    def run(self):
        """
        Create the PrismProject object.
        """
        # ------------------------------------------------------------------------------
        # Fire header events

        event_list, self.project_dir = self.fire_header_events()
        if self.project_dir is None:
            return prism.cli.base.TaskRunReturnResult(event_list, True)
        os.chdir(self.project_dir)
        event_list = fire_empty_line_event(event_list)

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

        # Return
        return prism.cli.base.TaskRunReturnResult(event_list, False)
