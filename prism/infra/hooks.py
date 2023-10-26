"""
PrismHooks class

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Standard library imports
import pandas as pd
from typing import Any, Optional
from pathlib import Path

# Prism-specific imports
from prism.cli.base import get_project_dir
from prism.infra import project as prism_project
import prism.constants
import prism.exceptions
import prism.prism_logging


####################
# Class definition #
####################

class PrismHooks:
    """
    PrismHooks class. This class is used to expose adapters defined within a profile to
    the user.
    """

    def __init__(self, project: prism_project.PrismProject):
        self.project = project

    def get_connection(self, adapter_name: str):
        """
        For SQL adapters, get the database connection:
            - BigQuery --> google.cloud.bigquery.Client
            - Postgres --> psycopg2.Connection
            - Redshift --> psycopg2.Connection
            - Snowflake --> snowflake.connector.Connection
            - Trino --> trino.dbapi.Connection
            - Presto --> prestodb.dbapi.Connection

        args:
            adapter_name: SQL adapter
        returns
            database connection as a class object
        """
        try:
            adapter_obj = self.project.adapters_object_dict[adapter_name]
        except KeyError:
            raise prism.exceptions.RuntimeException(
                message=f'adapter `{adapter_name}` not defined'
            )
        if not hasattr(adapter_obj, "engine"):
            raise prism.exceptions.RuntimeException(
                message=f'class for adapter `{adapter_name}` does not have `engine` attribute'  # noqa: E501
            )
        return adapter_obj.engine

    def get_cursor(self,
        adapter_name: str,
    ):
        """
        For SQL adapters, get a cursor object associated with the current database
        connection. This is only available for the following adapters:
            - Postgres
            - Redshift
            - Snowflake
            - Trino

        args:
            adapter_name: SQL adapter
        """
        conn = self.get_connection(adapter_name)
        return conn.cursor()

    def sql(self,
        adapter_name: str,
        query: str,
        return_type: Optional[str] = None
    ) -> Any:
        """
        Execute SQL query using adapter

        args:
            adapter: SQL adapter
            query: query to execute
            return_type: return type...accepted values are [`pandas`]
        returns:
            DataFrame containing results of SQL query
        """
        try:
            adapter_obj = self.project.adapters_object_dict[adapter_name]
        except KeyError:
            raise prism.exceptions.RuntimeException(
                message=f'adapter `{adapter_name}` not defined'
            )
        if not hasattr(adapter_obj, "execute_sql"):
            raise prism.exceptions.RuntimeException(
                message=f'class for adapter `{adapter_name}` does not have `execute_sql` method'  # noqa: E501
            )
        data = adapter_obj.execute_sql(query, return_type)
        return data

    def dbt_ref(self,
        adapter_name: str,
        target_1: str,
        target_2: Optional[str] = None,
        target_version: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get dbt task as a Pandas DataFrame

        args:
            adapter_name: name of dbt adapter in profile YML
            target_1: dbt task (or package)
            target_2: dbt task (if `target_1` is a package); default is None
        returns:
            dbt task as a Pandas DataFrame
        """
        try:
            dbt_project = self.project.adapters_object_dict[adapter_name]
        except KeyError:
            raise prism.exceptions.RuntimeException(
                message=f'adapter `{adapter_name}` not defined'
            )

        df = dbt_project.handle_ref(target_1, target_2, target_version)
        return df


# Function to load hooks in a script or environment
def load_hooks(project_dir: Optional[Path] = None):
    """
    Load the PrismHooks associated with the current project
    """
    if project_dir is None:
        project_dir = get_project_dir()
    project = prism_project.PrismProject(
        project_dir=project_dir,
        user_context={},
        which="run"
    )
    project.setup()

    # Hooks object
    hooks = PrismHooks(project)

    # Print a warning if the hooks are empty
    if hooks.project.adapters_object_dict == {}:
        print("WARNING: Your hooks are empty! Create a profile YAML to populate your hooks")  # noqa
    return hooks
