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


######################
## Class definition ##
######################

class Module01(prism.task.PrismTask):

    ## Run
    @prism.decorators.target(type=prism.target.PandasCsv, loc=prism_project.OUTPUT / 'sample_data_1.csv', index=False)
    @prism.decorators.target(type=prism.target.PandasCsv, loc=prism_project.OUTPUT / 'sample_data_2.csv', index=False)
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
        snowflake_query_1 = f"""
        SELECT 
            * 
        FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER" 
        WHERE 
            C_MKTSEGMENT = 'MACHINERY' 
        LIMIT 50
        """
        snowflake_1_df = hooks.sql(adapter_name="snowflake_profile_1", query=snowflake_query_1)

        snowflake_query_2 = f"""
        SELECT 
            * 
        FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER" 
        WHERE 
            C_MKTSEGMENT = 'HOUSEHOLD' 
        LIMIT 50
        """
        snowflake_2_df = hooks.sql(adapter_name="snowflake_profile_2", query=snowflake_query_2)

        return snowflake_1_df, snowflake_2_df


# EOF