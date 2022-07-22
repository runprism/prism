"""
Yaml parser class. This class is a child of the BaseParser class.

Table of Contents:
- Imports
- Class definition
"""


#############
## Imports ##
#############

# Standard library imports
import os
from pathlib import Path
import yaml
from typing import Any, Dict

# Prism imports
import prism.exceptions
from prism.parsers.base import BaseParser


######################
## Class definition ##
######################

class YamlParser(BaseParser):
    """
    Class for parsing Jinja in YAML files
    """


    def wkdir(self):
        """
        Return the directory of the YAML file calling this function. Can be called in YAML file via {{ wkdir() }}
        """
        return str(self.path.parent)


    def parent_dir(self,
        input_path: str
    ) -> str:
        """
        Return the parent directory {input_path}. Can be called in YAML file via {{ parent_dir(...) }}
        """
        path = Path(input_path)
        return str(path.parent)


    def concat(self,
        str1: str,
        str2: str
    ) -> str:
        """
        Concatenate {str1} and {str2}. Can be called in YAML file via {{ concat(...) }}
        """
        return str1+str2


    def env(self,
        var: str
    ) -> str:
        """
        Get environment variable {var}. Can be called in YAML file via {{ env(...) }}
        """
        env_var = os.getenv(var)
        if env_var is None:
            return ""
        else:
            return env_var

    
    def create_yml_dict(self,
        rendered_str: str
    ) -> Dict[Any, Any]:
        """
        Created dict representation of YAML file from rendered string

        args:
            rendered_str: rendered string
        return:
            yml_dict: YAML file represented as dictionary
        """
        temp_dict = yaml.safe_load(rendered_str)
        if temp_dict is None:
            return {}
        if not isinstance(temp_dict, dict):
            raise prism.exceptions.ParserException(message=f'error in YAML loading; invalid type `{str(type(temp_dict))}`')
        return temp_dict

    
    def parse(self) -> Dict[Any, Any]:
        """
        Parse YAML file with Jinja syntax

        args:
            None
        returns:
            yml_dict: YAML file represented as dictionary
        """
        # Define function dictionary
        func_dict = {
            "wkdir": self.wkdir,
            "parent_dir": self.parent_dir,
            "env": self.env,
            "concat": self.concat
        }

        # Rendered string
        rendered_string = self.render(self.path.parent, self.path.name, func_dict)

        # Return YAML dict
        yml_dict = self.create_yml_dict(rendered_string)
        return yml_dict


# EOF