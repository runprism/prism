"""
Class associated with `prism create task` CLI command.

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Prism-specific imports
import importlib
import prism.cli.base
import prism.mixins.create_task
import prism.mixins.compile
import prism.exceptions
import prism.constants
import prism.prism_logging
from prism.prism_logging import fire_console_event, fire_empty_line_event
from prism.event_managers.base import BaseEventManager, EventManagerOutput

# Other imports
from jinja2 import Environment, BaseLoader


####################
# Class definition #
####################

class CreateTaskTask(
    prism.cli.base.BaseTask,
    prism.mixins.compile.CompileMixin,
    prism.mixins.create_task.CreateTaskMixins
):
    """
    Class for creating tasks. This is accessed via the `prism create task`.
    """

    def run(self) -> prism.cli.base.TaskRunReturnResult:
        """
        Create new task(s) according to the user's specifications
        """

        # ------------------------------------------------------------------------------
        # Fire header events, get prism project

        task_return_result: prism.cli.base.TaskRunReturnResult = super().run()
        if task_return_result.has_error:
            return task_return_result
        event_list = task_return_result.event_list
        event_list = fire_empty_line_event(event_list)

        # ------------------------------------------------------------------------------
        # Define task type, task number, and task name

        task_type = self.args.type
        decorated = self.args.decorated

        # If adapter type is None, throw an error
        if task_type is None:
            e = prism.prism_logging.InvalidType(
                "task",
                prism.constants.VALID_TASK_TYPES
            )
            event_list = fire_console_event(e, event_list, 0, log_level='error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)

        # Grab the template
        if decorated:
            template_model = importlib.import_module(
                name=f"prism.templates.tasks.{task_type}_dec"
            )
        else:
            template_model = importlib.import_module(
                name=f"prism.templates.tasks.{task_type}_cls"
            )
        template = template_model.TEMPLATE
        task_template = Environment(loader=BaseLoader).from_string(template)  # type: ignore # noqa: E501

        # Get the number of tasks to create and the task name
        task_number = self.args.number
        user_task_name = self.args.name

        # Get directory. If it's blank, then new tasks should be dumped into the
        # `models/` directory. Otherwise, add the inputted directory to the models
        # directory.
        models_dir = self.get_models_dir(self.prism_project.project_dir)
        if self.args.dir == "":
            task_dir = models_dir
        else:
            task_dir = models_dir / self.args.dir

        # Fire events
        event_list = fire_console_event(
            prism.prism_logging.CreatingTasksEvent(),
            event_list,
            log_level='info'
        )

        # ------------------------------------------------------------------------------
        # Create tasks

        task_manager = BaseEventManager(
            idx=None,
            total=None,
            name='creating tasks',
            full_tb=self.args.full_tb,
            func=self.create_tasks
        )
        task_manager_output: EventManagerOutput = task_manager.manage_events_during_run(
            event_list=event_list,
            fire_exec_events=False,
            task_number=task_number,
            task_type=task_type,
            user_task_name=user_task_name,
            task_template=task_template,
            task_dir=task_dir,
            decorated=decorated,
        )
        output = task_manager_output.outputs
        event_to_fire = task_manager_output.event_to_fire
        event_list = task_manager_output.event_list
        if output == 0:
            event_list = fire_console_event(
                event_to_fire,
                event_list,
                log_level='error'
            )
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list, True)

        # Print output message if successfully executed
        event_list = fire_empty_line_event(event_list)
        event_list = fire_console_event(
            prism.prism_logging.TaskSuccessfulEndEvent(),
            event_list,
            0,
            log_level='info'
        )
        event_list = self.fire_tail_event(event_list)

        # Return
        return prism.cli.base.TaskRunReturnResult(event_list)
