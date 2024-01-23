"""
Snowflake adapter class definition

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Standard library imports
import pandas as pd
from typing import Any, Dict, Optional

# Prism-specific imports
from .adapter import Adapter
import prism.exceptions
from prism.utils import requires_dependencies


####################
# Class definition #
####################

class Snowflake(Adapter):

    def is_valid_config(self,
        config_dict: Dict[str, str],
        adapter_name: str,
        profile_name: str
    ) -> bool:
        """
        Check that config dictionary is profile YML is valid

        args:
            config_dict: config dictionary under snowflake adapter in profile YML
            adapter_name: name assigned to adapter
            profile_name: profile name containing adapter
        returns:
            boolean indicating whether config dictionary in profile YML is valid
        """

        # Required config vars
        required_config_vars = [
            'type',
            'user',
            'password',
            'account',
            'role',
            'warehouse',
            'database',
            'schema'
        ]

        # Raise an error if:
        #   1. Config doesn't contain any of the required vars or contains additional
        #      config vars
        #   2. Any of the config values are None
        actual_config_vars = []
        for k, v in config_dict.items():
            if k not in required_config_vars:
                raise prism.exceptions.InvalidProfileException(
                    message=f'invalid var `{k}` - see `{adapter_name}` adapter in `{profile_name}` profile in profile YML'  # noqa: E501
                )
            actual_config_vars.append(k)
            if v is None:
                raise prism.exceptions.InvalidProfileException(
                    message=f'`{k}` cannot be None - see `{adapter_name}` adapter in `{profile_name}` profile in profile YML'  # noqa: E501
                )
        vars_not_defined = list(set(required_config_vars) - set(actual_config_vars))
        if len(vars_not_defined) > 0:
            v = vars_not_defined.pop()
            raise prism.exceptions.InvalidProfileException(
                message=f'`{v}` must be defined - see `{adapter_name}` adapter in `{profile_name}` profile in profile YML'  # noqa: E501
            )

        # If no exception has been raised, return True
        return True

    @requires_dependencies(
        ["snowflake.connector", "pyarrow"],
        "snowflake"
    )
    def create_engine(self,
        adapter_dict: Dict[str, Any],
        adapter_name: str,
        profile_name: str
    ):
        """
        Parse Snowflake adapter, represented as a dict and return the Snowflake
        connector object

        args:
            adapter_dict: Snowflake adapter represented as a dictionary
            adapter_name: name assigned to adapter
            profile_name: profile name containing adapter
        returns:
            Snowflake connector object
        """
        # Import snowflake connector
        import snowflake.connector

        # Get configuration and check if config is valid
        self.is_valid_config(adapter_dict, adapter_name, profile_name)

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

    @requires_dependencies(
        ["snowflake.connector", "pyarrow"],
        "snowflake"
    )
    def execute_sql(self, query: str, return_type: Optional[str]) -> pd.DataFrame:
        """
        Execute the SQL query
        """
        # Create cursor for every SQL query -- this ensures thread safety
        cursor = self.engine.cursor()
        cursor.execute(query)

        # If the return type is `pandas`, then return a DataFrame
        if return_type == "pandas":
            df: pd.DataFrame = cursor.fetch_pandas_all()
            cursor.close()
            return df

        # Otherwise, just return the data
        else:
            data = cursor.fetchall()
            cursor.close()
            return data
