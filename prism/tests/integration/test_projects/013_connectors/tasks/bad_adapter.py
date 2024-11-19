from pathlib import Path

import prism.decorators
import prism.target

# Prism imports
import prism.task
from prism.runtime import Connection, Context


class BadAdapterTask(prism.task.PrismTask):
    # Run
    @prism.decorators.target(
        type=prism.target.PandasCsv,
        loc=Path(Context("OUTPUT")) / "bad_adapter.csv",
        index=False,
    )
    def run(self):
        sql = """
        SELECT
            *
        FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER"
        WHERE
            C_MKTSEGMENT = 'MACHINERY'
        LIMIT 50
        """
        conn = Connection("snowflake_connector")
        df = conn.execute_sql(sql=sql, return_type="pandas")
        return df
