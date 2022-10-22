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
import time
import pandas as pd
import pyspark.sql.functions as F


######################
## Class definition ##
######################

class Module02(prism.task.PrismTask):
    
    ## Run
    @prism.decorators.target(type=prism.target.PandasCsv, loc=prism_project.OUTPUT / 'sample_data_1_filtered.csv', index=False)
    @prism.decorators.target(type=prism.target.PandasCsv, loc=prism_project.OUTPUT / 'sample_data_2_filtered.csv', index=False)
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
        dfs = tasks.ref('module01.py')
        df_1_path = str(dfs[0])
        df_2_path = str(dfs[1])

        # Use spark to do some light processing
        df_1 = hooks.spark1.read.option("header", "true").csv(df_1_path)
        df_1_filtered = df_1.sort(F.col('C_ACCTBAL').asc()).filter(F.col('C_ACCTBAL')>1000)
        df_1_filtered_pd = df_1_filtered.toPandas()

        # Use spark to do some light processing
        df_2 = hooks.spark2.read.option("header", "true").csv(df_2_path)
        df_2_filtered = df_2.sort(F.col('C_ACCTBAL').asc()).filter(F.col('C_ACCTBAL')>2000)
        df_2_filtered_pd = df_2_filtered.toPandas()

        # Return
        return df_1_filtered_pd, df_2_filtered_pd






# EOF