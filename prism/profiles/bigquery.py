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
            'creds'
        ]

        # Raise an error if:
        #   1. Config doesn't contain any of the required vars or contains additional config vars
        #   2. Any of the config values are None
        actual_config_vars = []
        for k,v in config_dict.items():
            if k not in required_config_vars:
                raise prism.exceptions.InvalidProfileException(message=f'invalid var `{k}` under bigquery config in profile.yml')
            actual_config_vars.append(k)
            if v is None:
                raise prism.exceptions.InvalidProfileException(message=f'var `{k}` under bigquery config cannot be None in profile.yml')
        vars_not_defined = list(set(required_config_vars) - set(actual_config_vars))
        if len(vars_not_defined)>0:
            v = vars_not_defined.pop()
            raise prism.exceptions.InvalidProfileException(message=f'need to define `{v}` under bigquery config in profile.yml')

        # If no exception has been raised, return True
        return True


    def create_engine(self,
        adapter_dict: Dict[str, Any],
        adapter_type: str
    ):
        """
        Parse Google BigQuery adapter, represented as a dict and return the Google BigQuery connector object

        args:
            adapter_dict: Google BigQuery adapter represented as a dictionary
            adapter_type: type of adapter (will always be "bigquery")
            return_type: output type; one of either "str" or "list"
        returns:
            Snowflake connector object
        """
        # Import Python client for Google BigQuery
        from google.cloud import bigquery
        from google.oauth2 import service_account
        
        # Get configuration and check if config is valid
        self.is_valid_config(adapter_dict)
        credentials = service_account.Credentials.from_service_account_file(
            adapter_dict['creds']
        )

        # Connection
        ctx = bigquery.Client(credentials=credentials)
        return ctx


    def execute_sql(self, query: str) -> pd.DataFrame:
        """
        Execute the SQL query
        """
        # The Snowflake connection object behaves like a SQL alchemy engine. Therefore, we can use pd.read_sql(...)
        df =  self.engine.query(query).to_dataframe()
        return df


# EOF