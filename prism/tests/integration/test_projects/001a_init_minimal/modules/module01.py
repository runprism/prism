"""PRIVILEGED AND CONFIDENTIAL; FOR INTERNAL USE ONLY

In this script, we... 

--------------------------------------------------------------------------------
Table of Contents:
- Imports
- Class definition
    - Run
--------------------------------------------------------------------------------
"""

#############
## Imports ##
#############

import os
import prism_project                   # project directory is automatically added to sys.path
from prism.task import PrismTask       # Not necessary; prism infrastructure automatically imported on the back-end
import prism.target as PrismTarget     # Not necessary; prism infrastructure automatically imported on the back-end


######################
## Class definition ##
######################

class Module01(PrismTask):

    ## Run
    @PrismTask.target(type=PrismTarget.Txt, loc=os.path.join(prism_project.OUTPUT, 'hello_world.txt'))
    def run(self, mods, hooks):
        """
        Execute task.

        args:
            psm: built-in prism fns. These include:
                - psm.mod     --> for referencing output of other tasks
                - psm.sql     --> for executing sql query using an adapter in profile.yml
                - psm.spark   --> for accessing SparkSession (if pyspark specified in profile.yml)
                - psm.dbt_ref --> for getting dbt models as a pandas DataFrame
        returns:
            task output
        """
        return "Hello, world!"


# EOF