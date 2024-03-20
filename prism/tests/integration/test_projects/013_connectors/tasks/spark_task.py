from pathlib import Path

import pyspark.sql.functions as F

# Spark
from pyspark.sql import SparkSession

import prism.decorators
import prism.target

# Prism imports
import prism.task
from prism.runtime import CurrentRun

spark = (
    SparkSession.builder.appName("spark-test")
    .config("spark.driver.cores", 4)
    .config("spark.executor.cores", 4)
    .getOrCreate()
)


class PysparkTask(prism.task.PrismTask):
    # Run
    @prism.decorators.target(
        type=prism.target.PandasCsv,
        loc=Path(CurrentRun.ctx("OUTPUT")) / "machinery_sample_filtered.csv",
        index=False,
    )
    @prism.decorators.target(
        type=prism.target.PandasCsv,
        loc=Path(CurrentRun.ctx("OUTPUT")) / "household_sample_filtered.csv",
        index=False,
    )
    def run(self):
        dfs = CurrentRun.ref("snowflake_task.SnowflakeTask")
        machinery_df_pd = dfs[0]
        household_df_pd = dfs[1]

        # Use spark to do some light processing for machinery df
        machinery_df = spark.createDataFrame(machinery_df_pd)
        machinery_df_filtered = machinery_df.sort(F.col("C_ACCTBAL").asc()).filter(
            F.col("C_ACCTBAL") <= 1000
        )
        machinery_df_filtered_pd = machinery_df_filtered.toPandas()

        # Use spark to do some light processing for household df
        household_df = spark.createDataFrame(household_df_pd)
        household_df_filtered = (
            household_df.sort(F.col("C_ACCTBAL").asc())
            .filter(F.col("C_ACCTBAL") > 1000)
            .filter(F.col("C_ACCTBAL") <= 2000)
        )
        household_df_filtered_pd = household_df_filtered.toPandas()

        # Return
        return machinery_df_filtered_pd, household_df_filtered_pd
