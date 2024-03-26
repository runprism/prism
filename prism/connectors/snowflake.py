import pandas as pd
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

# Prism-specific imports
from prism.connectors.base import Connector
from prism.utils import requires_dependencies


class SnowflakeConnector(Connector):
    user: str
    password: str
    account: str
    role: str
    warehouse: str
    database: str
    schema: str

    # This should be an instance of the `snowflake.connector.Connection` class, but we
    # don't want to import snowflake.connector class unless the user
    # calls the `create_engine` method.
    engine: Any

    def __init__(
        self,
        id: str,
        user: str,
        password: str,
        account: str,
        role: str,
        warehouse: str,
        database: str,
        schema: str,
    ):
        super().__init__(
            id,
            user=user,
            password=password,
            account=account,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
        )

        self.engine = self.create_engine()

    @requires_dependencies(["snowflake.connector", "pyarrow"], "snowflake")
    def create_engine(self) -> Any:
        """
        Create the Snowflake connection
        """
        import snowflake.connector

        conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            schema=self.schema,
            warehouse=self.warehouse,
            role=self.role,
        )
        return conn

    @requires_dependencies(["snowflake.connector", "pyarrow"], "snowflake")
    def execute_sql(
        self,
        sql: str,
        return_type: Optional[Literal["pandas"]],
    ) -> Union[pd.DataFrame, List[Tuple[Any]], List[Dict[Any, Any]]]:
        # For type hinting
        import snowflake.connector

        # Create cursor for every SQL query -- this ensures thread safety
        cursor: snowflake.connector.cursor.SnowflakeCursor = self.engine.cursor()
        cursor.execute(sql)

        # If the return type is `pandas`, then return a DataFrame
        if return_type == "pandas":
            df: pd.DataFrame = cursor.fetch_pandas_all()
            cursor.close()
            return df

        # Otherwise, just return the data
        else:
            data = cursor.fetchall()
            cursor.close()
            return data
