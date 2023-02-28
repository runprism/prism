"""
Mixins use to create tasks

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Standard library imports
from pathlib import Path
from typing import Dict

# Prism-specific imports
import prism.cli.base
import prism.cli.compile
import prism.exceptions
import prism.constants
import prism.logging


####################
# Class definition #
####################

class TaskMixins:
    """
    Mixin for Task task
    """

    def user_task_name_to_classname(self,
        task_type: str,
        user_task_name: str,
    ):
        f"""
        The user-inputted `task_name` corresponds to the file name for the new task. We
        want to use this same `task_name` to populate the class name associated with the
        new task.

        args:
            task_type: one of {','.join(prism.constants.VALID_TASK_TYPES)}
            user_task_name: user-inputted `task_name` argument
        returns:
            class name
        """
        if task_type == "sql":
            return ""

        else:
            # Convert to proper case
            delim = "_"
            new_sections = []
            for name_section in user_task_name.split(delim):
                new_sections.append(name_section[0].upper() + name_section[1:])
            name_proper_case = delim.join(new_sections)

            # Remove "_"
            class_name = name_proper_case.replace("_", "")
            return class_name

    def create_task_modules(self,
        task_type: str,
        task_template: str,
        args: Dict[str, str],
        user_task_name: str,
        task_dir: Path,
    ) -> Path:
        f"""
        Create the standalone module associated with the new task

        args:
            task_type: one of {','.join(prism.constants.VALID_TASK_TYPES)}
            task_template: task Jinja2 template
            args: arguments to populate template
            user_task_name: user-inputted task name argument. This will be the task's
                            filename
        returns:
            file path of newly created file
        """
        # Identify file extension to use
        if task_type == "sql":
            extension = "sql"
        else:
            extension = "py"

        # Render the template
        rendered_template = task_template.render(args)

        # Write the file
        filename = f"{user_task_name}.{extension}"
        if not task_dir.is_dir():
            task_dir.mkdir(parents=True, exist_ok=True)

        # If the file already exists, then throw an error
        if Path(task_dir / filename).is_file():
            raise prism.exceptions.TaskAlreadyExistsException(
                message=f"task `{str(task_dir.name)}/{filename}` already exists"
            )
        with open(task_dir / filename, 'w') as f:
            f.write(rendered_template)

    def create_tasks(self,
        task_number: int,
        task_type: str,
        user_task_name: str,
        task_template: str,
        task_dir: Path,
    ):
        f"""
        Create new tasks

        args:
            task_number: number of tasks to create
            task_type: one of {','.join(prism.constants.VALID_TASK_TYPES)}
            user_task_name: user-inputted task name
            task_template: unrendered Jinja2 template
            task_dir: directory to place new tasks in
        returns:
            None
        """
        # Only one task is requested
        if task_number == 1:
            template_args = {
                "task_cls_name": self.user_task_name_to_classname(
                    task_type, user_task_name
                )
            }
            self.create_task_modules(
                task_type,
                task_template,
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
                    task_template,
                    template_args,
                    new_user_task_name,
                    task_dir
                )
