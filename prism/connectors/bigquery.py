# Standard library imports
from pathlib import Path
from typing import Any, List, Literal, Optional, Tuple, Union

import pandas as pd

from prism.utils import requires_dependencies

# Prism-specific imports
from .base import Connector


class BigQueryConnector(Connector):
    creds: Union[str, Path]

    # This should be an instance of the `bigquery.Client` class, but we don't want to
    # import bigquery class unless the user calls the `create_engine` method.
    engine: Any

    def __init__(self, id: str, creds: Union[str, Path]):
        super().__init__(
            id,
            creds=creds,
        )

    @requires_dependencies(["google.cloud", "google.oauth2"], "bigquery")  # noqa
    def create_engine(self):
        """
        Parse Google BigQuery adapter, represented as a dict and return the Google
        BigQuery connector object

        args:
            adapter_dict: Google BigQuery adapter represented as a dictionary
            adapter_name: name assigned to adapter
            profile_name: profile name containing adapter
        returns:
            Snowflake connector object
        """
        # Import Python client for Google BigQuery
        from google.cloud import bigquery
        from google.oauth2 import service_account

        # Get configuration and check if config is valid
        credentials = service_account.Credentials.from_service_account_file(self.creds)

        # Connection
        ctx = bigquery.Client(credentials=credentials)
        return ctx

    @requires_dependencies(["google.cloud", "google.oauth2"], "bigquery")  # noqa
    def execute_sql(
        self,
        sql: str,
        return_type: Optional[Literal["pandas"]],
    ) -> Union[pd.DataFrame, Any]:
        """
        Execute the SQL query
        """
        # Type hinting is kind of a pain here, so ignore for now.
        job = self.engine.query(sql)
        if return_type == "pandas":
            df: pd.DataFrame = job.to_dataframe()
            return df
        data = job.result()
        res: List[Tuple[Any, ...]] = []
        for row in data:
            res.append(row)
        return res
