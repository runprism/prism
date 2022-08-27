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
from pyspark.sql.types import StructType, StructField, StringType
import prism_project
from prism.task import PrismTask
from prism.target import target, PySparkParquet


######################
## Class definition ##
######################

class Module01(PrismTask):

    ## Run
    @target(type=PySparkParquet, loc=os.path.join(prism_project.OUTPUT, 'target_parquet'), mode='overwrite')
    def run(self, psm):
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
        # Define schema
        schema = StructType([
            StructField('col1', StringType(), True),
            StructField('col2', StringType(), True),
            StructField('col3', StringType(), True)
        ])

        # Define data
        data = [
            ('col1_value1', 'col2_value1', 'col3_value1'),
            ('col1_value2', 'col2_value2', 'col3_value2'),
            ('col1_value3', 'col2_value3', 'col3_value3'),
            ('col1_value4', 'col2_value4', 'col3_value4'),
            ('col1_value5', 'col2_value5', 'col3_value5'),
            ('col1_value6', 'col2_value6', 'col3_value6')
        ]

        # Load data into schema
        df = psm.spark.createDataFrame(data, schema)
        return df


# EOF