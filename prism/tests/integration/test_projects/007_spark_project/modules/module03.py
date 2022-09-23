#############
## Imports ##
#############

# Prism infrastructure imports
import prism.task
import prism.target
import prism.decorators

# Prism project imports
import prism_project

# Other imports
import pyspark.sql.functions as F


######################
## Class definition ##
######################

class Module03(prism.task.PrismTask):

    ## Run
    def run(self, tasks, hooks):
        """
        Execute task.

        args:
            tasks: used to reference output of other tasks --> tasks.ref('...')
            hooks: built-in Prism hooks. These include:
                - hooks.dbt_ref --> for getting dbt models as a pandas DataFrame
                - hooks.sql     --> for executing sql query using an adapter in profile.yml
                - hooks.spark   --> for accessing SparkSession (if pyspark specified in profile.yml)
        returns:
            task output
        """
        df = hooks.spark.read.parquet(tasks.ref('module02.py'))
        df_new = df.filter(F.col('col1')>=F.lit('col1_value3'))
        return df_new


# EOF