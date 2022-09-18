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

# This section is not strictly necessary, because all prism-related imports are automatically handled on the back-end.
# Nevertheless, we include them here to facilitate using the prism tasks and targets.

from prism.task import PrismTask
import prism.target as PrismTarget


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