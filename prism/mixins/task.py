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
import yaml
import shutil
from pathlib import Path
from typing import Any, Dict

# Prism-specific imports
import prism.cli.base
import prism.cli.compile
import prism.exceptions
import prism.constants
import prism.logging
import prism.templates.tasks as tasks_templates_dir
from prism.triggers import PrismTrigger


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
        # Create 
        if task_type == "sql":
            extension = "sql"
        else:
            extension = "py"
        
        # Render the template
        rendered_template = task_template.render(args)

        # Write the file
        filename = f"{user_task_name}.{extension}"
        if task_dir.is_dir():
            task_dir.mkdir(parents=True, exist_ok=True)
        with open(task_dir / filename, 'w') as f:
            f.write(rendered_template)
