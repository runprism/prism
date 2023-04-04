"""
Base parser class. Parsers are used to parse Jinja in YAML files (e.g., profile YML).

Table of Contents:
- Imports
- Class definition
"""


###########
# Imports #
###########

# Standard library imports
# import importlib
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from typing import Any, Dict

# Prism-sepcific imports
import prism.exceptions


####################
# Class definition #
####################

class BaseParser:
    """
    Base class for defining parser behavior
    """

    def __init__(self,
        path: Path,
        prism_project=None
    ):
        self.path = path
        self.prism_project = prism_project

    def render(self,
        parent_path: Path,
        filename: str,
        func_dict: Dict[Any, Any]
    ) -> str:
        """
        Interpret/execute Jinja syntax in {filename} and return {filename} as a string

        args:
            parent_path: path containing YAML file (for loading Environment)
            filename: name of template
            func_dict: function dictionary for Jinja globals
        returns:
            rendered_string: {filename} with executed Jinja
        """
        # Load environment and template
        env = Environment(loader=FileSystemLoader(str(parent_path)))
        jinja_template = env.get_template(filename)
        self.globals = jinja_template.globals

        # Store the path of the file itself in `__file__`
        self.globals["__file__"] = str(self.path)

        # Store the prism project, if it exists
        if self.prism_project is not None:
            self.globals["prism_project"] = self.prism_project.run_context['prism_project']  # noqa: E501

        # Update template globals with inputted function dictinoary
        jinja_template.globals.update(func_dict)

        # Render string
        rendered_string = jinja_template.render()
        if not isinstance(rendered_string, str):
            raise prism.exceptions.ParserException(
                message=f'invalid return type `{str(type(rendered_string))}`'
            )
        return rendered_string

    def parse(self):
        raise prism.exceptions.ParserException(message='`parse` not implemented')
