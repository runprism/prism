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
REDSHIFT_CONFIGURATION_OPTIONS_FROM_DOCS = {
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

ADDITIONAL_CONFIGS = {
    'autocommit': bool
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
            if k not in list(REDSHIFT_CONFIGURATION_OPTIONS_FROM_DOCS.keys()) and k not in list(ADDITIONAL_CONFIGS.keys()):
                raise prism.exceptions.InvalidProfileException(message=f'invalid var `{k}` under redshift config in profile.yml')
            
            if k in list(REDSHIFT_CONFIGURATION_OPTIONS_FROM_DOCS.keys()):
                if not isinstance(v, REDSHIFT_CONFIGURATION_OPTIONS_FROM_DOCS[k]):
                    raise prism.exceptions.InvalidProfileException(message=f'var `{k}` is an invalid type, must be {str(REDSHIFT_CONFIGURATION_OPTIONS_FROM_DOCS[k])}')
            elif k in list(ADDITIONAL_CONFIGS.keys()):
                if not isinstance(v, ADDITIONAL_CONFIGS[k]):
                    raise prism.exceptions.InvalidProfileException(message=f'var `{k}` is an invalid type, must be {str(ADDITIONAL_CONFIGS[k])}')

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
        connection_code_list = []
        for k,v in adapter_dict.items():
            if k in list(REDSHIFT_CONFIGURATION_OPTIONS_FROM_DOCS.keys()):
                if isinstance(v, str):
                    connection_code_list.append(f"{k}='{adapter_dict[k]}'")
                else:
                    connection_code_list.append(f"{k}={adapter_dict[k]}")
        connection = ',\n'.join(connection_code_list)
        ctx_str_list = [
            'import redshift_connector',
            f'ctx = redshift_connector.connect({connection})'
        ]
        exec('\n'.join(ctx_str_list))
        
        # Create cursor object
        cursor = locals()['ctx'].cursor()
        
        # Autocommit. If no autocommit is specified, then set to True
        try:
            autocommit_config = adapter_dict['autocommit']
            cursor.autocommit = autocommit_config
        except KeyError:
            cursor.autocommit = True
        return cursor


    def execute_sql(self, query: str, return_type: str) -> pd.DataFrame:
        """
        Execute the SQL query
        """
        # The engine is the Redshift cursor
        self.engine.execute(query)
        self.engine.close()
        if return_type=="pandas":
            df: pd.DataFrame = self.engine.fetch_dataframe()
            return df


# EOF