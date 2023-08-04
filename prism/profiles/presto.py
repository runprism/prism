"""
Presto adapter class definition

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
import prestodb

# Prism-specific imports
from .adapter import Adapter
import prism.exceptions


####################
# Class definition #
####################

class Presto(Adapter):

    def is_valid_config(self,
        config_dict: Dict[str, str],
        adapter_name: str,
        profile_name: str,
    ) -> bool:
        """
        Check that config dictionary is profile YML is valid

        args:
            config_dict: config dictionary under PostgresQL adapter in profile YML
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
            'port',
            'host',
        ]

        # Optional config vars
        optional_config_vars = [
            'http_scheme',
            'catalog',
            'schema',
        ]

        # Raise an error if:
        #   1. Config doesn't contain any of the required vars or contains additional
        #      config vars
        #   2. Any of the config values are None
        actual_config_vars = []
        for k, v in config_dict.items():
            if k not in required_config_vars and k not in optional_config_vars:
                raise prism.exceptions.InvalidProfileException(
                    message=f'invalid var `{k}` - see `{adapter_name}` adapter in `{profile_name}` profile in profile YML'  # noqa: E501
                )
            if k in required_config_vars:
                actual_config_vars.append(k)
            if v is None:
                raise prism.exceptions.InvalidProfileException(
                    message=f'var `{k}` cannot be None - see `{adapter_name}` adapter in `{profile_name}` profile in profile YML'  # noqa: E501
                )
        vars_not_defined = list(set(required_config_vars) - set(actual_config_vars))
        if len(vars_not_defined) > 0:
            v = vars_not_defined.pop()
            raise prism.exceptions.InvalidProfileException(
                message=f'var `{v}` must be defined - see `{adapter_name}` adapter in `{profile_name}` profile in profile YML'  # noqa: E501
            )

        # If the schema is specified, then Trino requires that the catalog is also
        # specified. However, the user can choose to only specify the catalog if they
        # choose.
        catalog_schema_specs = {}
        for var in ['schema', 'catalog']:
            catalog_schema_specs[var] = (
                var in list(config_dict.keys())
                and (  # noqa: W503
                    config_dict[var] is not None or config_dict[var] != ""
                )
            )
        if catalog_schema_specs['schema'] and not catalog_schema_specs['catalog']:
            raise prism.exceptions.InvalidProfileException(
                message=f"`schema` is set but `catalog` is not in `{profile_name}` profile in profile YML"  # noqa: E501
            )

        # If no exception has been raised, return True
        return True

    def create_engine(self,
        adapter_dict: Dict[str, Any],
        adapter_name: str,
        profile_name: str
    ):
        """
        Parse PostgresQL adapter, represented as a dict and return the PostgresQL
        connector object

        args:
            adapter_dict: PostgresQL adapter represented as a dictionary
            adapter_name: name assigned to adapter
            profile_name: profile name containing adapter
        returns:
            PostgresQL connector object
        """

        # Get configuration and check if config is valid
        self.is_valid_config(adapter_dict, adapter_name, profile_name)

        # Schema is present. Since this is a valid config dict, then we know that
        # catalog must be present.
        if 'schema' in list(adapter_dict.keys()):
            conn = prestodb.dbapi.connect(
                host=adapter_dict['host'],
                port=adapter_dict['port'],
                http_scheme=adapter_dict.get('http_schema', 'https'),
                auth=prestodb.auth.BasicAuthentication(
                    adapter_dict['user'],
                    adapter_dict['password']
                ),
                catalog=adapter_dict['catalog'],
                schema=adapter_dict['schema']
            )

        # Just catalog is present
        elif 'catalog' in list(adapter_dict.keys()):
            conn = prestodb.dbapi.connect(
                host=adapter_dict['host'],
                port=adapter_dict['port'],
                http_scheme=adapter_dict.get('http_schema', 'https'),
                auth=prestodb.auth.BasicAuthentication(
                    adapter_dict['user'],
                    adapter_dict['password']
                ),
                catalog=adapter_dict['catalog']
            )

        # Neither catalog nor schema is present
        else:
            conn = prestodb.dbapi.connect(
                host=adapter_dict['host'],
                port=adapter_dict['port'],
                http_scheme=adapter_dict.get('http_schema', 'https'),
                auth=prestodb.auth.BasicAuthentication(
                    adapter_dict['user'],
                    adapter_dict['password']
                )
            )

        return conn

    def execute_sql(self, query: str, return_type: Optional[str]) -> pd.DataFrame:
        """
        Execute the SQL query
        """
        # Create cursor for every SQL query -- this ensures thread safety
        cursor = self.engine.cursor()
        cursor.execute(query)
        data = cursor.fetchall()

        # If the return type is `pandas`, then return a DataFrame
        if return_type == "pandas":
            cols = []
            for elts in cursor.description:
                cols.append(elts[0])
            df: pd.DataFrame = pd.DataFrame(data=data, columns=cols)
            cursor.close()
            return df

        # Otherwise, return the data as it exists
        else:
            cursor.close()
            return data
