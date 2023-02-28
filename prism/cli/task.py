"""
SparkSubmit class definition, called via `prism spark-submit`

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
import prism.mixins.task
import prism.mixins.compile
import prism.exceptions
import prism.constants
import prism.logging
from prism.event_managers.base import BaseEventManager
from prism.logging import fire_console_event, fire_empty_line_event
from jinja2 import Environment, BaseLoader

####################
# Class definition #
####################

class TaskTask(
    prism.cli.base.BaseTask,
    prism.mixins.compile.CompileMixin,
    prism.mixins.task.TaskMixins):
    """
    Class for create tasks. This is accessed via the `prism task`.
    """

    def run(self) -> prism.cli.base.TaskRunReturnResult:
        """
        Execute triger task
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

        # If adapter type is None, throw an error
        if task_type is None:
            e = prism.logging.InvalidType(
                "task",
                prism.constants.VALID_TASK_TYPES
            )
            event_list = fire_console_event(e, event_list, 0, log_level='error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)

        # Grab the template
        template_module = importlib.import_module(
            name=f"prism.templates.tasks.{task_type}"
        )
        template = template_module.TEMPLATE
        rtemplate = Environment(loader=BaseLoader).from_string(template)

        # Get the number of tasks to create and the task name
        task_number = self.args.number
        user_task_name = self.args.name

        # Get directory. If it's blank, then new tasks should be dumped into the
        # `modules/` directory. Otherwise, add the inputted directory to the modules
        # directory.
        modules_dir = self.get_modules_dir(self.prism_project.project_dir)
        if self.args.dir == "":
            task_dir = modules_dir
        else:
            task_dir = modules_dir / self.args.dir

        # Only one task is requested
        if task_number == 1:
            template_args = {
                "task_cls_name": self.user_task_name_to_classname(task_type, user_task_name)
            }
            self.create_task_modules(
                task_type,
                rtemplate,
                template_args,
                user_task_name,
                task_dir
            )
        
        # Multiple tasks are requested
        else:
            for i in range(1, task_number + 1):
                
                # Add the task number to the class name and the user task name
                cls_name = self.user_task_name_to_classname(task_type, user_task_name)
                cls_name += str(i)
                template_args = {
                    "task_cls_name": cls_name
                }
                new_user_task_name = user_task_name + f"_{i}"

                # Create task modules
                self.create_task_modules(
                    task_type,
                    rtemplate,
                    template_args,
                    new_user_task_name,
                    task_dir
                )


            
