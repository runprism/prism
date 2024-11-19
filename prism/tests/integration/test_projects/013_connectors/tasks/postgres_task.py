from pathlib import Path

import prism.decorators
import prism.target

# Prism imports
import prism.task
from prism.runtime import Connection, Context


class PostgresTask(prism.task.PrismTask):
    # Run
    @prism.decorators.target(
        type=prism.target.PandasCsv,
        loc=Path(Context("OUTPUT")) / "sample_postgres_data.csv",
        index=False,
    )
    def run(self):
        sql = """
        SELECT
            first_name
            , last_name
        FROM us500
        ORDER BY
            first_name
            , last_name
        LIMIT 10
        """
        conn = Connection("postgres-connector")
        df = conn.execute_sql(sql=sql, return_type="pandas")
        return df
