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

# Prism-specific imports
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

    def sql(self,
        adapter_name: str,
        query: str,
        return_type: str = "pandas"
    ) -> Any:
        """
        Execute SQL query using adapter

        args:
            adapter: SQL adapter
            query: query to execute
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
        df = adapter_obj.execute_sql(query, return_type)
        if return_type == "pandas":
            return df

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
