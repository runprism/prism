import pandas as pd
from typing import Any, List, Literal, Optional, Tuple, Union

# Prism-specific imports
from prism.utils import requires_dependencies
from prism.connectors.base import Connector


class PostgresConnector(Connector):
    user: str
    password: str
    port: int
    host: str
    database: str
    autocommit: bool

    # This should be an instance of the `psycopg2.extensions.connection`, but we don't
    # want to import psycopg2 unless the user creates calls the `create_engine` method.
    engine: Any

    def __init__(
        self,
        id: str,
        user: str,
        password: str,
        port: int,
        host: str,
        database: str,
        autocommit: bool = True,
    ):
        super().__init__(
            id,
            user=user,
            password=password,
            port=port,
            host=host,
            database=database,
            autocommit=autocommit,
        )

        # Create engine
        self.engine = self.create_engine()

    @requires_dependencies("psycopg2", "postgres")
    def create_engine(self) -> Any:
        """
        Create the Postgres connection using `psycopg2`
        """
        import psycopg2

        conn = psycopg2.connect(
            dbname=self.database,
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
        )
        conn.set_session(autocommit=self.autocommit)
        return conn

    @requires_dependencies("psycopg2", "postgres")
    def execute_sql(
        self,
        sql: str,
        return_type: Optional[Literal["pandas"]],
    ) -> Union[pd.DataFrame, List[Tuple[Any, ...]]]:
        # For type hinting
        import psycopg2

        # Create cursor for every SQL query -- this ensures thread safety
        cursor: psycopg2.extensions.cursor = self.engine.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()

        # If the return type is `pandas`, then return a DataFrame
        if return_type == "pandas":
            cols = []
            for elts in cursor.description:
                cols.append(elts[0])
            df: pd.DataFrame = pd.DataFrame(data=data, columns=cols)
            cursor.close()
            return df

        # Otherwise, return the data as it exists
        else:
            cursor.close()
            return data  # type: ignore
