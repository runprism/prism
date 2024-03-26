from pathlib import Path

import prism.decorators
import prism.target

# Prism imports
import prism.task
from prism.runtime import CurrentRun


class SnowflakeTask(prism.task.PrismTask):
    # Run
    @prism.decorators.target(
        type=prism.target.PandasCsv,
        loc=Path(CurrentRun.ctx("OUTPUT")) / "machinery_sample.csv",
        index=False,
    )
    @prism.decorators.target(
        type=prism.target.PandasCsv,
        loc=Path(CurrentRun.ctx("OUTPUT")) / "household_sample.csv",
        index=False,
    )
    def run(self):
        conn = CurrentRun.conn("snowflake-connector")

        machinery_sql = """
        SELECT
            *
        FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER"
        WHERE
            c_mktsegment = 'MACHINERY'
        ORDER BY
            c_custkey
        LIMIT 50
        """
        machinery_df = conn.execute_sql(sql=machinery_sql, return_type="pandas")

        household_sql = """
        SELECT
            *
        FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER"
        WHERE
            c_mktsegment = 'HOUSEHOLD'
        ORDER BY
            c_custkey
        LIMIT 50
        """
        household_df = conn.execute_sql(sql=household_sql, return_type="pandas")

        return machinery_df, household_df
