"""
Wrapper for pyspark job. Needed to ensure compatibility with spark-submit.

Table of Contents:
- Imports
- Class definition
"""

#############
## Imports ##
#############

# Standard package imports
import os
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
import tempfile
from typing import Any, Dict, List, Tuple

# Prism-specific imports
import prism.exceptions


##########################
## Path of current file ##
##########################

WRAPPER_DIR = os.path.dirname(__file__)


######################
## Class definition ##
######################

class SparkWrapper:

    def __init__(self,
        args_dict: Dict[str, Any],
        command: str = 'spark-submit'
    ):  
        self.template_parent_path = Path(WRAPPER_DIR)
        self.script_name = 'script.py'
        self.args_dict = args_dict
        self.command = command


    def populate_template(self,
        template_parent_path: Path,
        template_file_name: str,
        args_dict: Dict[str, Any]
    ) -> str:
        """
        Populate Python file template found at {template_parent_path} / {template_file_name} using {args_dict}

        args:
            template_parent_path: path containing template
            template_file_name: file name of template
            args_dict: dictionary used to populate Jinja2 template
        returns:
            rendered_string: rendered string
        """
        env = Environment(loader=FileSystemLoader(template_parent_path))
        jinja_template = env.get_template(template_file_name)
        rendered_string = jinja_template.render(args_dict)
        if not isinstance(rendered_string, str):
            raise prism.exceptions.RuntimeException(message=f'invalid `rendered_string`; expected type `str`, got `{type(str(rendered_string))}`')
        return rendered_string


    def create_python_file(self,
        code: str
    ) -> str:
        """
        Create temporary Python file with {code}. This file will be used to submit the PySpark job.

        args:
            code: code to place in Python file
        returns:
            path: path to tmpfile
        """
        fd, path = tempfile.mkstemp(suffix = '.py')
        with open(path, 'w') as f:
            f.write(code)
        os.close(fd)
        return path


    def bash_command(self) -> Tuple[str, List[str]]:
        """
        Create bash command for executing remote job

        args:
            None
        returns:
            list of arguments for subprocess
        """
        populated_template_str = self.populate_template(self.template_parent_path, self.script_name, self.args_dict)
        tmpfile = self.create_python_file(populated_template_str)

        # Return bash command
        bash_command_list = [self.command, tmpfile]
        return tmpfile, bash_command_list


# EOF