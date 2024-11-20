from typing import Any, Dict, List, Union

from prism.connectors.base import Connector


class _Connection:
    """
    THIS CLASS SHOULD NEVER BE IMPORTED DIRECTLY. INSTEAD, IMPORT `Connection`, i.e.,
    ```python
        from prism.runtime import Connection
    ```

    Connection stores Singleton connections to external data sources, e.g., Snowflake,
    Redshift, Postgres, etc.ß
    """

    run_id: str
    _data: Dict[str, Any]
    connectors: Dict[str, Connector]

    def __init__(self):
        pass

    def _set_connectors(self, connectors: List[Connector]):
        self.connectors: Dict[str, Connector] = {}
        for conn in connectors:
            self.connectors[conn.id] = conn

    def _setup(
        self,
        run_id: str,
        connectors: List[Union[Connector]],
    ):
        self.run_id = run_id
        self._set_connectors(connectors)

    def __call__(self, connector_id: str) -> Connector:
        """
        Get the connector object associated with ID `connector_id`. These are defined in
        the client's instantiation.

        args:
            connector_id: ID of task from which to retrieve output
        returns:
            connector object associated with `connector_id`
        raises:
            prism.exception.ConnectorDoesNotExistException if the connector ID is not
            found
        """
        if connector_id not in self.connectors.keys():
            raise ValueError(
                f"connector ID `{connector_id}` not found run `{self.run_id}`!"
            )
        return self.connectors[connector_id]


if __name__ != "__main__":
    Connection: _Connection = _Connection()
