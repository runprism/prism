"""
In this script, we... 
"""

#############
## Imports ##
#############

import os
import prism_project
from prism.task import PrismTask
from prism.target import target


######################
## Class definition ##
######################

class Module03(PrismTask):
    
    ## Run
    def run(self, mods, hooks):
        """
        Execute task.

        args:
            mods: object used to access the output of other tasks, e.g.,:
                mods.ref('some_other_task.py')
            hooks: hooks used to augment Prism functionality. These include:
                hooks.sql     --> for executing sql query using an adapter in profile.yml
                hooks.spark   --> for accessing SparkSession (if pyspark specified in profile.yml)
                hooks.dbt_ref --> for getting dbt models as a pandas DataFrame
        returns:
            task output
        """
        # TODO: Implement the `run` method
        return None


# EOF