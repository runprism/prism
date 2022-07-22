"""
PySpark adapter class definition

Table of Contents
- Imports
- Class definition
"""

#############
## Imports ##
#############

# Standard library imports
from typing import Any, Dict, List, Union

# Prism-specific imports
from .adapter import Adapter
import prism.exceptions


######################
## Class definition ##
######################

class Pyspark(Adapter):
    """
    Class for creating a SparkSession within a prism project
    """

    def get_alias(self):
        return self.get_adapter_var(self.adapter_dict, "alias", self.type)

	
    def base_config_template(self):
        pyspark_config_exec_template = [
            "{alias} = SparkSession.builder.enableHiveSupport() \\",
            "{config_vars}",
            "\t.getOrCreate()"
        ]
        return pyspark_config_exec_template


    def parse_adapter_dict(self,
        adapter_dict: Dict[str, Any],
        adapter_type: str,
        return_type: str = "list"
    ) -> Union[str, List[str]]:
        """
        Parse PySpark adapter, represented as a dict

        args:
            adapter_dict: PySpark adapter represented as a dictionary
            adapter_type: type of adapter (it will always be PySpark)
            type: output type; one of either "str" or "list"
        returns:
            config_exec: code to execute PySpark configuration
        """
        if return_type not in ["str", "list"]:
            raise prism.exceptions.RuntimeException(message=f'invalid `{return_type}` in `{self.__class__.__name__}.parse_adapter_dict`, must be either "str" or "list"')

        base = self.base_config_template()

        # Get alias and config dictionary
        alias = self.get_adapter_var(adapter_dict, "alias", adapter_type)
        config = self.get_adapter_var(adapter_dict, "config", adapter_type)

        # Iterate through config and create SparkContext builder
        config_idx = base.index("{config_vars}")
        config_list = [f"\t.config('{k}', '{v}') \\" for k,v in config.items() if v is not None]
        base[config_idx:config_idx+1] = config_list
        profile_exec_list = []
        for elem in base:
            new_elem = elem.format(
                alias=alias
            )
            profile_exec_list.append(new_elem)
        
        # Return either list or string
        if return_type=="list":
            return profile_exec_list
        else:
            return '\n'.join(profile_exec_list)


    def create_engine(self, adapter_dict: Dict[str, Any], type: str):
        from pyspark.sql import SparkSession
        execution_code = self.parse_adapter_dict(adapter_dict, type, "str")
        
        # For type hinting
        if isinstance(execution_code, list):
            execution_code = '\n'.join(execution_code)
        exec(execution_code, locals())
        
        try:
            log_level = self.get_adapter_var(adapter_dict, "loglevel", type)
        except prism.exceptions.InvalidProfileException:
            log_level = "WARN"
        
        if log_level not in ['ALL', 'DEBUG', 'ERROR', 'FATAL', 'INFO', 'OFF', 'TRACE', 'WARN']:
                raise prism.exceptions.InvalidProfileException(message=f'invalid log level `{log_level}`')

        # For type hinting
        alias = self.get_adapter_var(adapter_dict, "alias", type)
        if not isinstance(alias, str):
            raise prism.exceptions.InvalidProfileException(message=f'invalid `alias` type')
        locals()[alias].sparkContext.setLogLevel(log_level)
        return locals()[alias]


# EOF