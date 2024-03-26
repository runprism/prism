import pandas as pd
from typing import Any, Union, Optional


class Connector:
    id: str
    engine: Any

    def __init__(self, id: str, **kwargs):
        self.id = id
        for k, v in kwargs.items():
            setattr(self, k, v)

    def create_engine(self):
        raise NotImplementedError

    def execute_sql(
        self, sql: str, return_type: Optional[str]
    ) -> Union[pd.DataFrame, Any]:
        raise NotImplementedError
