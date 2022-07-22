"""
PrismFunctions class

Table of Contents
- Imports
- Class definition
"""

#############
## Imports ##
#############

# Standard library imports
import pandas as pd
from typing import Any, Dict, Optional

# Prism-specific imports
from prism.infra import project as prism_project
import prism.constants
import prism.exceptions
import prism.logging


######################
## Class definition ##
######################


class PrismFunctions:
    """
    Functions accessible via `psm` object in PrismTasks
    """

    def __init__(self, project: prism_project.PrismProject, upstream: Dict[Any, Any]):
        self.project = project
        self.upstream = upstream


    def mod(self, module: str):
        return self.upstream[module].get_output()


    def sql(self,
        adapter: str,
        query: str
    ) -> pd.DataFrame:
        """
        Execute SQL query using adapter

        args:
            adapter: SQL adapter
            query: query to execute
        returns:
            DataFrame containing results of SQL query
        """
        if adapter not in prism.constants.VALID_SQL_ADAPTERS:
            raise prism.exceptions.RuntimeException(message=f'invalid SQL adapter `{adapter}`')
        try:
            adapter_obj = self.project.adapters_object_dict[adapter]
        except KeyError:
            raise prism.exceptions.RuntimeException(message=f'adapter `{adapter}` not defined')
        if not hasattr(adapter_obj, "execute_sql"):
            raise prism.exceptions.RuntimeException(message=f'class for adapter `{adapter}` does not have `execute_sql` method')
        df = adapter_obj.execute_sql(query)
        return df

    
    def dbt_ref(self, 
        target_1: str,
        target_2: Optional[str] = None
    ) -> pd.DataFrame:
        try:
            dbt_project = self.project.adapters_object_dict['dbt']
        except KeyError:
            raise prism.exceptions.RuntimeException(message=f'adapter `dbt` not defined')
        
        df = dbt_project.handle_ref(target_1, target_2)
        return df


# EOF