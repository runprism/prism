"""
Redshift adapter class definition

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Standard library imports
import pandas as pd
from typing import Any, Dict
import psycopg2

# Prism-specific imports
from .adapter import Adapter
import prism.exceptions


####################
# Class definition #
####################

class Redshift(Adapter):

    def is_valid_config(self,
        config_dict: Dict[str, str],
        adapter_name: str,
        profile_name: str,
    ) -> bool:
        """
        Check that config dictionary is profile.yml is valid

        args:
            config_dict: config dictionary under Redshift adapter in profile.yml
            adapter_name: name assigned to adapter
            profile_name: profile name containing adapter
        returns:
            boolean indicating whether config dictionary in profile.yml is valid
        """

        # Required config vars
        required_config_vars = [
            'type',
            'user',
            'password',
            'port',
            'host',
            'database'
        ]

        # Optional config vars
        optional_config_vars = [
            'autocommit'
        ]

        # Raise an error if:
        #   1. Config doesn't contain any of the required vars or contains additional
        #      config vars
        #   2. Any of the config values are None
        actual_config_vars = []
        for k, v in config_dict.items():
            if k not in required_config_vars and k not in optional_config_vars:
                raise prism.exceptions.InvalidProfileException(
                    message=f'invalid var `{k}` - see `{adapter_name}` adapter in `{profile_name}` profile in profile.yml'  # noqa: E501
                )
            if k in required_config_vars:
                actual_config_vars.append(k)
            if v is None:
                raise prism.exceptions.InvalidProfileException(
                    message=f'var `{k}` cannot be None - see `{adapter_name}` adapter in `{profile_name}` profile in profile.yml'  # noqa: E501
                )
        vars_not_defined = list(set(required_config_vars) - set(actual_config_vars))
        if len(vars_not_defined) > 0:
            v = vars_not_defined.pop()
            raise prism.exceptions.InvalidProfileException(
                message=f'var `{v}` must be defined - see `{adapter_name}` adapter in `{profile_name}` profile in profile.yml'  # noqa: E501
            )

        # If no exception has been raised, return True
        return True

    def create_engine(self,
        adapter_dict: Dict[str, Any],
        adapter_name: str,
        profile_name: str
    ):
        """
        Parse Redshift adapter, represented as a dict and return the Redshift connector
        object

        args:
            adapter_dict: Redshift adapter represented as a dictionary
            adapter_name: name assigned to adapter
            profile_name: profile name containing adapter
        returns:
            Redshift connector object
        """

        # Get configuration and check if config is valid
        self.is_valid_config(adapter_dict, adapter_name, profile_name)

        # Create psycopg2 connection
        conn = psycopg2.connect(
            dbname=adapter_dict['database'],
            host=adapter_dict['host'],
            port=adapter_dict['port'],
            user=adapter_dict['user'],
            password=adapter_dict['password']
        )

        # Autocommit. If no autocommit is specified, then set to True
        try:
            autocommit_config = bool(adapter_dict['autocommit'])
            conn.set_session(autocommit=autocommit_config)
        except KeyError:
            conn.set_session(autocommit=True)
        return conn

    def execute_sql(self, query: str, return_type: str) -> pd.DataFrame:
        """
        Execute the SQL query
        """
        # Create cursor for every SQL query -- this ensures thread safety
        cursor = self.engine.cursor()
        cursor.execute(query)
        if return_type == "pandas":
            data = cursor.fetchall()
            cols = []
            for elts in cursor.description:
                cols.append(elts[0])
            df: pd.DataFrame = pd.DataFrame(data=data, columns=cols)
            cursor.close()
            return df
        else:
            # Fetch one to ensure that the query was executed
            cursor.fetchone()
            cursor.close()
