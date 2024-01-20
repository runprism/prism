"""
PySpark adapter class definition

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Standard library imports
from typing import Any, Dict, List, Union

# Prism-specific imports
from .adapter import Adapter
import prism.exceptions
from prism.utils import requires_dependencies


####################
# Class definition #
####################

class Pyspark(Adapter):
    """
    Class for creating a SparkSession within a prism project
    """

    def get_alias(self):
        return self.get_adapter_var(
            self.adapter_dict, "alias", self.name, self.profile_name
        )

    def base_config_template(self):
        pyspark_config_exec_template = [
            "{alias} = SparkSession.builder.enableHiveSupport() \\",
            "{config_vars}",
            "\t.getOrCreate()"
        ]
        return pyspark_config_exec_template

    def parse_adapter_dict(self,
        adapter_dict: Dict[str, Any],
        adapter_name: str,
        profile_name: str,
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
            raise prism.exceptions.RuntimeException(
                message=f'invalid `{return_type}` in `{self.__class__.__name__}.parse_adapter_dict`, must be either "str" or "list"'  # noqa: E501
            )

        base = self.base_config_template()

        # Get alias and config dictionary
        alias = self.get_adapter_var(
            adapter_dict, "alias", adapter_name, profile_name
        )
        config = self.get_adapter_var(
            adapter_dict, "config", adapter_name, profile_name
        )

        # Iterate through config and create SparkContext builder
        config_idx = base.index("{config_vars}")
        config_list = [
            f"\t.config('{k}', '{v}') \\" for k, v in config.items() if v is not None
        ]
        base[config_idx:config_idx + 1] = config_list
        profile_exec_list = []
        for elem in base:
            new_elem = elem.format(
                alias=alias
            )
            profile_exec_list.append(new_elem)

        # Return either list or string
        if return_type == "list":
            return profile_exec_list
        else:
            return '\n'.join(profile_exec_list)

    @requires_dependencies(
        "pyspark",
        "pyspark",
    )
    def create_engine(self,
        adapter_dict: Dict[str,
        Any],
        adapter_name: str,
        profile_name: str
    ):
        from pyspark.sql import SparkSession  # noqa: F401
        execution_code = self.parse_adapter_dict(
            adapter_dict, adapter_name, profile_name, "str"
        )

        # For type hinting
        if isinstance(execution_code, list):
            execution_code = '\n'.join(execution_code)
        exec(execution_code, locals())

        try:
            log_level = self.get_adapter_var(
                adapter_dict, "loglevel", adapter_name, profile_name
            )
        except prism.exceptions.InvalidProfileException:
            log_level = "WARN"

        accepted_log_levels = [
            'ALL',
            'DEBUG',
            'ERROR',
            'FATAL',
            'INFO',
            'OFF',
            'TRACE',
            'WARN'
        ]
        if log_level not in accepted_log_levels:
            raise prism.exceptions.InvalidProfileException(
                message=f'invalid log level `{log_level}`'
            )

        # For type hinting
        alias = self.get_adapter_var(adapter_dict, "alias", adapter_name, profile_name)
        if not isinstance(alias, str):
            raise prism.exceptions.InvalidProfileException(
                message='invalid `alias` type'
            )
        locals()[alias].sparkContext.setLogLevel(log_level)
        return locals()[alias]
