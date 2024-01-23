
###########
# Imports #
###########

# Prism infrastructure imports
import prism.task
import prism.target
import prism.decorators

# Prism project imports
import prism_project


####################
# Class definition #
####################

class PostgresTask(prism.task.PrismTask):

    # Run
    @prism.decorators.target(
        type=prism.target.PandasCsv,
        loc=prism_project.OUTPUT / 'sample_postgres_data.csv',
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
        df = hooks.sql(
            adapter_name="postgres_base",
            query=sql,
            return_type="pandas"
        )
        return df
