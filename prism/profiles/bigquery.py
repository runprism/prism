"""
BigQuery adapter class definition

Table of Contents
- Imports
- Class definition
"""

#############
## Imports ##
#############

# Standard library imports
from typing import Any, Dict
import pandas as pd

# Prism-specific imports
from .adapter import Adapter
import prism.exceptions


######################
## Class definition ##
######################

class BigQuery(Adapter):
    """
    Class for connecting prism project to BigQuery
    """


    def is_valid_config(self,
        config_dict: Dict[str, str],
        adapter_name: str,
        profile_name: str
    ) -> bool:
        """
        Check that config dictionary is profile.yml is valid

        args:
            config_dict: config dictionary under snowflake adapter in profile.yml
            adapter_name: name assigned to adapter
            profile_name: profile name containing adapter
        returns:
            boolean indicating whether config dictionary in profile.yml is valid
        """

        # Required config vars
        required_config_vars = [
            'type',
            'creds'
        ]

        # Raise an error if:
        #   1. Config doesn't contain any of the required vars or contains additional config vars
        #   2. Any of the config values are None
        actual_config_vars = []
        for k,v in config_dict.items():
            if k not in required_config_vars:
                raise prism.exceptions.InvalidProfileException(message=f'invalid var `{k}` - see `{adapter_name}` adapter in `{profile_name}` profile in profile.yml')
            actual_config_vars.append(k)
            if v is None:
                raise prism.exceptions.InvalidProfileException(message=f'var `{k}` cannot be None - see `{adapter_name}` adapter in `{profile_name}` profile in profile.yml')
        vars_not_defined = list(set(required_config_vars) - set(actual_config_vars))
        if len(vars_not_defined)>0:
            v = vars_not_defined.pop()
            raise prism.exceptions.InvalidProfileException(message=f'var `{v}` must be defined - see `{adapter_name}` adapter in `{profile_name}` profile in profile.yml')

        # If no exception has been raised, return True
        return True


    def create_engine(self,
        adapter_dict: Dict[str, Any],
        adapter_name: str,
        profile_name: str
    ):
        """
        Parse Google BigQuery adapter, represented as a dict and return the Google BigQuery connector object

        args:
            adapter_dict: Google BigQuery adapter represented as a dictionary
            adapter_name: name assigned to adapter
            profile_name: profile name containing adapter
        returns:
            Snowflake connector object
        """
        # Import Python client for Google BigQuery
        from google.cloud import bigquery
        from google.oauth2 import service_account
        
        # Get configuration and check if config is valid
        self.is_valid_config(adapter_dict, adapter_name, profile_name)
        credentials = service_account.Credentials.from_service_account_file(
            adapter_dict['creds']
        )

        # Connection
        ctx = bigquery.Client(credentials=credentials)
        return ctx


    def execute_sql(self, query: str, return_type: str) -> pd.DataFrame:
        """
        Execute the SQL query
        """
        # The Snowflake connection object behaves like a SQL alchemy engine. Therefore, we can use pd.read_sql(...)
        df =  self.engine.query(query).to_dataframe()
        if return_type=="pandas":
            return df


# EOF