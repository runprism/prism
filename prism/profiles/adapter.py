"""
Adapter class, borrowing from the dbt terminology

Table of Contents
- Imports
- Class definition
"""

#############
## Imports ##
#############

# Standard library imports
from typing import Any, Dict, Union

# Prism-specific imports
from .meta import MetaAdapter
import prism.exceptions


######################
## Class definition ##
######################

class Adapter(metaclass=MetaAdapter):
    
    def __init__(self, type: str, adapter_dict: Dict[str, Any], create_engine=True):
        """
        Adapter instantiation

        args:
            type: adapter type
            adapter_dict: configuration dictionary
            create_engine: boolean for whether to create engine. Should only be false for testing
        """
        self.type = type
        self.adapter_dict = adapter_dict

        # Create the SQL engine to execute queries
        if create_engine:
            self.engine = self.create_engine(self.adapter_dict, self.type)


    def create_engine(self,
        adapter_dict: Dict[str, Any],
        adapter_type: str
    ):
        raise prism.exceptions.RuntimeException(message=f"`create_engine` not implemented in class `{self.__class__.__name__}`")


    def get_adapter_dict(self):
        return self.adapter_dict

    
    def get_adapter_var(self,
        adapter_dict: Dict[str, Any],
        var: str,
        adapter_type: str
    ) -> Union[Any, Dict[Any, Any]]:
        """
        Parse adapter dictionary and return inputted `var`

        args:
            adapter_dict: adapter from profile.yml file represented as dict
            var: var to retrieve
            adapter_type: profile type
        returns:
            adapter_var: adapter_var
        """
        for k,v in adapter_dict.items():
            if k==var:
                if v is None:
                    raise prism.exceptions.InvalidProfileException(message=f'`{var}` cannot be None in `{adapter_type}` profile in profile.yml')
                return v
        raise prism.exceptions.InvalidProfileException(message=f'`{var}` not found in `{adapter_type}` profile in profile.yml')


    def parse_config(self):
        raise prism.exceptions.InvalidProfileException(message=f'`parse_config` method not implemented')



# EOF