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

import os
import pyspark.sql.functions as F
import prism_project
from prism.task import PrismTask       # Not necessary; prism infrastructure automatically imported on the back-end
import prism.target as PrismTarget     # Not necessary; prism infrastructure automatically imported on the back-end


######################
## Class definition ##
######################

class Module02(PrismTask):

    ## Run
    @PrismTask.target(type=PrismTarget.PySparkParquet, loc=os.path.join(prism_project.OUTPUT, 'module02'), mode='overwrite')
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
        df = hooks.spark.read.parquet(mods.ref('module01.py'))
        df_new = df.filter(F.col('col1')>=F.lit('col1_value2'))
        return df_new


# EOF