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

import pyspark.sql.functions as F
import prism_project
from prism.task import PrismTask       # Not necessary; prism infrastructure automatically imported on the back-end
import prism.target as PrismTarget     # Not necessary; prism infrastructure automatically imported on the back-end


######################
## Class definition ##
######################

class Module03(PrismTask):

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
        df = psm.spark.read.parquet(mods.ref('module02.py'))
        df_new = df.filter(F.col('col1')>=F.lit('col1_value3'))
        return df_new


# EOF