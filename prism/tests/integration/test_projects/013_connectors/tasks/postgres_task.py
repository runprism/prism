from pathlib import Path

# Prism imports
import prism.task
import prism.target
import prism.decorators
from prism.runtime import CurrentRun


class PostgresTask(prism.task.PrismTask):

    # Run
    @prism.decorators.target(
        type=prism.target.PandasCsv,
        loc=Path(CurrentRun.ctx("OUTPUT")) / "sample_postgres_data.csv",
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
        conn = CurrentRun.conn("postgres-connector")
        df = conn.execute_sql(sql=sql, return_type="pandas")
        return df
