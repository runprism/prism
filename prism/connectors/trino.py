import pandas as pd
from typing import Any, List, Literal, Optional, Union

# Prism-specific imports
from prism.connectors.base import Connector
from prism.utils import requires_dependencies


####################
# Class definition #
####################


class TrinoConnector(Connector):
    user: str
    password: str
    port: int
    host: str
    http_scheme: Optional[str]
    catalog: Optional[str]
    schema: Optional[str]

    # This should be an instance of the `trino.dbapi.Connection`, but we don't want to
    # import trino unless the user creates calls the `create_engine` method.
    engine: Any

    def __init__(
        self,
        id: str,
        user: str,
        password: str,
        port: int,
        host: str,
        http_scheme: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        super().__init__(
            id,
            user=user,
            password=password,
            port=port,
            host=host,
            http_scheme=http_scheme,
            catalog=catalog,
            schema=schema,
        )

        # Minor validation
        if self.schema is not None:
            if self.catalog is None:
                raise ValueError(
                    "`catalog` cannot be `None` when `schema` is specified"
                )  # noqa: E501

        # Create engine
        self.engine = self.create_engine()

    @requires_dependencies(
        "trino",
        "trino",
    )
    def create_engine(self) -> Any:
        """
        Create the Trino connection
        """
        import trino

        if self.schema is not None:
            conn = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                http_scheme=self.http_scheme if self.http_scheme else "https",
                auth=trino.auth.BasicAuthentication(
                    self.user,
                    self.password,
                ),
                catalog=self.catalog,
                schema=self.schema,
            )

        # Just catalog is present
        elif self.catalog is not None:
            conn = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                http_scheme=self.http_scheme if self.http_scheme else "https",
                auth=trino.auth.BasicAuthentication(
                    self.user,
                    self.password,
                ),
                catalog=self.catalog,
            )

        # Neither catalog nor schema is present
        else:
            conn = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                http_scheme=self.http_scheme if self.http_scheme else "https",
                auth=trino.auth.BasicAuthentication(
                    self.user,
                    self.password,
                ),
            )

        return conn

    @requires_dependencies(
        "trino",
        "trino",
    )
    def execute_sql(
        self,
        sql: str,
        return_type: Optional[Literal["pandas"]],
    ) -> Union[pd.DataFrame, List[List[Any]]]:
        # For type hinting
        import trino

        # Create cursor for every SQL query -- this ensures thread safety
        cursor: trino.dbapi.Cursor = self.engine.cursor()
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
        else:
            cursor.close()
            return data  # type: ignore
