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

# From https://docs.aws.amazon.com/redshift/latest/mgmt/python-configuration-options.html
ACCEPTABLE_ADAPTER_KEYS = {
    'access_key_id': str,
    'allow_db_user_override': bool,
    'app_name': str,
    'auth_profile': str,
    'auto_create': bool,
    'client_id': str,
    'client_secret': str,
    'cluster_identifier': str,
    'credentials_provider': str,
    'database': str,
    'database_metadata_current_db_only': bool,
    'db_groups': str,
    'db_user': str,
    'endpoint_url': str,
    'group_federation': bool,
    'host': str,
    'iam': bool,
    'iam_disable_cache': bool,
    'idpPort': int,
    'idp_response_timeout': int,
    'idp_tenant': str,
    'listen_port': int,
    'login_url': str,
    'max_prepared_statements': int,
    'numeric_to_float': bool,
    'partner_sp_id': str,
    'password': str,
    'port': int,
    'preferred_role': str,
    'principal_arn': str,
    'profile': str,
    'provider_name': str,
    'region': str,
    'role_arn': str,
    'role_session_name': str,
    'scope': str,
    'secret_access_key_id': str,
    'session_token': str,
    'serverless_acct_id': str,
    'serverless_work_group': str,
    'ssl': bool,
    'ssl_insecure': bool,
    'sslmode': str,
    'timeout': int,
    'user': str,
    'web_identity_token': str
}


######################
## Class definition ##
######################

class Redshift(Adapter):


    def is_valid_config(self,
        config_dict: Dict[str, str]
    ) -> bool:
        """
        Check that config dictionary for Redshift adapter only contains valid keys

        args:
            config_dict: config dictionary under redshift adapter in profile.yml
        returns:
            boolean indicating whether config dictionary in profile.yml is valid
        """
        # Raise an error if config dictionary contains keys not in ACCEPTABLE_ADAPTER_KEYS or if the value
        # is not the correct type
        for k,v in config_dict.items():
            if k not in list(ACCEPTABLE_ADAPTER_KEYS.keys()):
                raise prism.exceptions.InvalidProfileException(message=f'invalid var `{k}` under redshift config in profile.yml')
            if not isinstance(v, ACCEPTABLE_ADAPTER_KEYS[k]):
                raise prism.exceptions.InvalidProfileException(message=f'var `{k}` is an invalid type, must be {str(ACCEPTABLE_ADAPTER_KEYS[k])}')

        # If no exception has been raised, return True
        return True


    def create_engine(self,
        adapter_dict: Dict[str, Any],
        adapter_type: str
    ):
        """
        Parse Redshift adapter, represented as a dict and return the Redshift connector object

        args:
            adapter_dict: Redshift adapter represented as a dictionary
        returns:
            Redshift connector object
        """
        
        # Get configuration and check if config is valid
        self.is_valid_config(adapter_dict)

        # Connection. Define the connection code as a string so that we can parse the YAML dict and create the
        # configuration programatically.
        connection = ',\n'.join([f"{k}={adapter_dict[k]}"])
        ctx_str_list = [
            'import redshift_connector',
            f'ctx = redshift_connector.connect({connection})'
        ]

        #TODO: implement engine



    def execute_sql(self, query: str) -> pd.DataFrame:
        """
        Execute the SQL query
        """
        # The Snowflake connection object behaves like a SQL alchemy engine. Therefore, we can use pd.read_sql(...)
        df =  pd.read_sql(query, self.engine)
        return df


# EOF