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
from .. import prism_project
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
            mods: used to reference output of other tasks --> mods.ref('...')
            hooks: built-in Prism hooks. These include:
                - hooks.dbt_ref --> for getting dbt models as a pandas DataFrame
                - hooks.sql     --> for executing sql query using an adapter in profile.yml
                - hooks.spark   --> for accessing SparkSession (if pyspark specified in profile.yml)
        returns:
            task output
        """
        return "Hello, world!"

# EOF