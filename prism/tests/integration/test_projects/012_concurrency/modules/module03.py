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
import pandas as pd
import prism_project
from prism.task import PrismTask       # Not necessary; prism infrastructure automatically imported on the back-end
import prism.target as PrismTarget     # Not necessary; prism infrastructure automatically imported on the back-end


######################
## Class definition ##
######################

class Module03(PrismTask):

    def get_txt_output(self, path):
        with open(path) as f:
            lines = f.read()
        f.close()
        return lines
    
    ## Run
    def run(self, mods, hooks):
        """
        Execute task.

        args:
            psm: built-in prism fns. These include:
                - psm.mod     --> for referencing output of other tasks
                - psm.dbt_ref --> for getting dbt models as a pandas DataFrame
                - psm.sql     --> for executing sql query using an adapter in profile.yml
                - psm.spark   --> for accessing SparkSession (if pyspark specified in profile.yml)
        returns:
            task output
        """
        module1_times = pd.read_csv(mods.ref('module01.py'))
        module2_times = pd.read_csv(mods.ref('module02.py'))
        return 'Hello from module 3!'


# EOF