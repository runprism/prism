"""
Snowflake adapter class definition

Table of Contents
- Imports
- Class definition
"""

#############
## Imports ##
#############

# Standard library imports
import pandas as pd
from typing import Any, Dict, Union

# Prism-specific imports
from .adapter import Adapter
import prism.exceptions


######################
## Class definition ##
######################

class Snowflake(Adapter):


    def is_valid_config(self,
        config_dict: Dict[str, str]
    ) -> bool:
        """
        Check that config dictionary is profile.yml is valid

        args:
            config_dict: config dictionary under snowflake adapter in profile.yml
        returns:
            boolean indicating whether config dictionary in profile.yml is valid
        """

        # Required config vars
        required_config_vars = [
            'user',
            'password',
            'account',
            'role',
            'warehouse',
            'database',
            'schema'
        ]

        # Raise an error if:
        #   1. Config doesn't contain any of the required vars or contains additional config vars
        #   2. Any of the config values are None
        actual_config_vars = []
        for k,v in config_dict.items():
            if k not in required_config_vars:
                raise prism.exceptions.InvalidProfileException(message=f'invalid var `{k}` under snowflake config in profile.yml')
            actual_config_vars.append(k)
            if v is None:
                raise prism.exceptions.InvalidProfileException(message=f'var `{k}` under snowflake config cannot be None in profile.yml')
        vars_not_defined = list(set(required_config_vars) - set(actual_config_vars))
        if len(vars_not_defined)>0:
            v = vars_not_defined.pop()
            raise prism.exceptions.InvalidProfileException(message=f'need to define `{v}` under snowflake config in profile.yml')

        # If no exception has been raised, return True
        return True


    def create_engine(self,
        adapter_dict: Dict[str, Any],
        adapter_type: str
    ):
        """
        Parse Snowflake adapter, represented as a dict and return the Snowflake connector object

        args:
            adapter_dict: Snowflake adapter represented as a dictionary
            return_type: output type; one of either "str" or "list"
        returns:
            Snowflake connector object
        """
        # Import snowflake connector
        import snowflake.connector

        # Get configuration and check if config is valid
        self.is_valid_config(adapter_dict)

        # Connection
        ctx = snowflake.connector.connect(
            account=adapter_dict['account'],
            user=adapter_dict['user'],
            password=adapter_dict['password'],
            database=adapter_dict['database'],
            schema=adapter_dict['schema'],
            warehouse=adapter_dict['warehouse'],
            role=adapter_dict['role']
        )
        return ctx


    def execute_sql(self, query: str) -> pd.DataFrame:
        """
        Execute the SQL query
        """
        # The Snowflake connection object behaves like a SQL alchemy engine. Therefore, we can use pd.read_sql(...)
        df =  pd.read_sql(query, self.engine)
        return df


# EOF