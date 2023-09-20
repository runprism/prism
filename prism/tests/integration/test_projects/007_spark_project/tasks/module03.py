###########
# Imports #
###########

# Prism infrastructure imports
import prism.task
import prism.target
import prism.decorators

# Other imports
import pyspark.sql.functions as F


####################
# Class definition #
####################

class Task03(prism.task.PrismTask):

    # Run
    def run(self, tasks, hooks):
        """
        Execute task.

        args:
            tasks: used to reference output of other tasks --> tasks.ref('...')
            hooks: built-in Prism hooks. These include:
            - hooks.dbt_ref --> for getting dbt tasks as a pandas DataFrame
            - hooks.sql     --> for executing sql query using an adapter in profile YML
            - hooks.spark   --> for accessing SparkSession
        returns:
            task output
        """
        df = tasks.ref('module02.py')
        df_new = df.filter(F.col('col1') >= F.lit('col1_value3'))
        return df_new
