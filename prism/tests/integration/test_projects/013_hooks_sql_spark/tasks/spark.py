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
import pyspark.sql.functions as F


####################
# Class definition #
####################

class PysparkTask(prism.task.PrismTask):

    # Run
    @prism.decorators.target(
        type=prism.target.PandasCsv,
        loc=prism_project.OUTPUT / 'machinery_sample_filtered.csv',
        index=False
    )
    @prism.decorators.target(
        type=prism.target.PandasCsv,
        loc=prism_project.OUTPUT / 'household_sample_filtered.csv',
        index=False
    )
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
        dfs = tasks.ref('snowflake.py')
        machinery_df_pd = dfs[0]
        household_df_pd = dfs[1]

        # Use spark to do some light processing for machinery df
        machinery_df = hooks.spark.createDataFrame(machinery_df_pd)
        machinery_df_filtered = machinery_df.sort(F.col('C_ACCTBAL').asc()) \
            .filter(F.col('C_ACCTBAL') <= 1000)
        machinery_df_filtered_pd = machinery_df_filtered.toPandas()

        # Use spark to do some light processing for household df
        household_df = hooks.spark.createDataFrame(household_df_pd)
        household_df_filtered = household_df.sort(F.col('C_ACCTBAL').asc()) \
            .filter(F.col('C_ACCTBAL') > 1000) \
            .filter(F.col('C_ACCTBAL') <= 2000)
        household_df_filtered_pd = household_df_filtered.toPandas()

        # Return
        return machinery_df_filtered_pd, household_df_filtered_pd
