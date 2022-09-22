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

class Module04(PrismTask):

    ## Run
    @PrismTask.target(type=PrismTarget.PySparkParquet, loc=os.path.join(prism_project.OUTPUT, 'module04'), mode='overwrite')
    def run(self, tasks, hooks):
        df_new = tasks.ref('module03.py').filter(F.col('col1')>=F.lit('col1_value4'))
        return df_new


# EOF