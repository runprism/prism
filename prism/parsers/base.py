"""
Base parser class. Parsers are used to parse Jinja in YAML files (e.g., profile.yml).

Table of Contents:
- Imports
- Class definition
"""


#############
## Imports ##
#############

# Standard library imports
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from typing import Any, Dict

# Prism-sepcific imports
import prism.exceptions


######################
## Class definition ##
######################

class BaseParser:
    """
    Base class for defining parser behavior
    """

    def __init__(self,
        path: Path
    ):
        self.path = path


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
        
        # Update template globals with inputted function dictinoary
        jinja_template.globals.update(func_dict)

        # Render string
        rendered_string = jinja_template.render()
        if not isinstance(rendered_string, str):
            raise prism.exceptions.ParserException(message=f'invalid return type `{str(type(rendered_string))}`') 
        return rendered_string
    

    def parse(self):
        raise prism.exceptions.ParserException(message='`parse` not implemented')


# EOF