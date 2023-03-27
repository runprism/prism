###########
# Imports #
###########

# Prism infrastructure imports
import prism.task
import prism.target
import prism.decorators

# Prism project imports
import prism_project

# Other imports
from pyspark.sql.types import StructType, StructField, StringType


######################
## Class definition ##
######################

class Module01(prism.task.PrismTask):

    ## Run
    @prism.decorators.target(type=prism.target.PySparkParquet, loc=str(prism_project.OUTPUT / 'target_parquet'), mode='overwrite')
    def run(self, tasks, hooks):
        """
        Execute task.

        args:
            tasks: used to reference output of other tasks --> tasks.ref('...')
            hooks: built-in Prism hooks. These include:
                - hooks.dbt_ref --> for getting dbt models as a pandas DataFrame
                - hooks.sql     --> for executing sql query using an adapter in profile YML
                - hooks.spark   --> for accessing SparkSession (if pyspark specified in profile YML)
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
        df = hooks.spark.createDataFrame(data, schema)
        return df


# EOF