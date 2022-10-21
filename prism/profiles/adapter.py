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
    
    def __init__(self, name: str, adapter_dict: Dict[str, Any], profile_name: str, create_engine=True):
        """
        Adapter instantiation

        args:
            name: adapter name
            adapter_dict: configuration dictionary
            profile_name: named profile containing adapter
            create_engine: boolean for whether to create engine. Should only be false for testing
        """
        self.name = name
        self.adapter_dict = adapter_dict
        self.profile_name = profile_name

        # Create the SQL engine to execute queries
        if create_engine:
            self.engine = self.create_engine(self.adapter_dict, self.name, self.profile_name)


    def create_engine(self,
        adapter_dict: Dict[str, Any],
        adapter_name: str,
        profile_name: str
    ):
        raise prism.exceptions.RuntimeException(message=f"`create_engine` not implemented in class `{self.__class__.__name__}`")


    def get_adapter_dict(self):
        return self.adapter_dict

    
    def get_adapter_var(self,
        adapter_dict: Dict[str, Any],
        var: str,
        adapter_name: str,
        profile_name: str
    ) -> Union[Any, Dict[Any, Any]]:
        """
        Parse adapter dictionary and return inputted `var`

        args:
            adapter_dict: adapter from profile.yml file represented as dict
            var: var to retrieve
            adapter_name: adapter name
            profile_name: name of profile containing adapter
        returns:
            adapter_var: adapter_var
        """
        for k,v in adapter_dict.items():
            if k==var:
                if v is None:
                    raise prism.exceptions.InvalidProfileException(message=f'`{var}` cannot be None - see `{adapter_name}` adapter in `{profile_name}` profile in profile.yml')
                return v
        raise prism.exceptions.InvalidProfileException(message=f'`{var}` not found - see `{adapter_name}` adapter in `{profile_name}` profile in profile.yml')


    def parse_config(self):
        raise prism.exceptions.InvalidProfileException(message=f'`parse_config` method not implemented')



# EOF