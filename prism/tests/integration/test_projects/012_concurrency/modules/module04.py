"""PRIVILEGED AND CONFIDENTIAL; FOR INTERNAL USE ONLY

In this script, we... 

--------------------------------------------------------------------------------
Table of Contents:
- Imports
- Class definition
    - Section 1 Title
    - Section 2 Title
    ...
    - Run
--------------------------------------------------------------------------------
"""

#############
## Imports ##
#############

import prism_project
from prism.task import PrismTask       # Not necessary; prism infrastructure automatically imported on the back-end
import prism.target as PrismTarget     # Not necessary; prism infrastructure automatically imported on the back-end


######################
## Class definition ##
######################

class Module04(PrismTask):

    ## Run
    def run(self, mods, hooks):
        """
        Execute task.

        args:
            mods: used to reference output of other tasks --> mods.ref('...')
            hooks: built-in Prism hooks. These include:
                - hooks.dbt_ref --> for getting dbt models as a pandas DataFrame
                - hooks.sql     --> for executing sql query using an adapter in profile.yml
                - hooks.spark   --> for accessing SparkSession (if pyspark specified in profile.yml)
        returns:
            task output
        """
        return mods.ref('module03.py') + "\n" + "Hello from module 4!"


# EOF