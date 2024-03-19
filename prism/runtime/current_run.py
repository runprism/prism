from typing import Any, Dict, List, Union, Optional


# Prism-specific imports
from prism.connectors.base import Connector


class _CurrentRun:
    """
    THIS CLASS SHOULD NEVER BE IMPORTED DIRECTLY. INSTEAD, IMPORT `CurrentRun`, i.e.,
    ```python
        from prism.runtime import CurrentRun
    ```

    CurrentRun stores contextual variables for that tasks can access during runtime.
    This includes variables that the user specifies in their client definition and
    connectors.
    """

    run_id: str
    _data: Dict[str, Any]
    _refs: Dict[str, Any]
    connectors: Dict[str, Connector]

    def __init__(self, data: Dict[str, Any]):
        self._data = data
        self._refs = {}
        self.connectors = {}

    def _set_connectors(self, connectors: List[Connector]):
        for conn in connectors:
            self.connectors[conn.id] = conn

    def _setup(
        self,
        run_id: str,
        runtime_ctx: Dict[str, Any],
        connectors: List[Union[Connector]],
    ):
        self.run_id = run_id
        self._data.update(runtime_ctx)
        self._set_connectors(connectors)

    def ctx(self, key: str, default_value: Optional[Any] = None) -> Any:
        """
        Get the value associated with context variable `key`. Context variables can be
        set in two places: when instantiated the PrismProject (with the `ctx` keyword
        argument) and when creating the run job (with the `runtime_ctx` keyword argument
        in the PrismProject's `create_run_job` method).

        args:
            key: variable to retrieve
            default_value: default value to return if `key` is not found.
                Default is `None`
        returns:
            value associated with context variable `key`
        """
        return self._data.get(key, default_value)

    def _set_task_output_value(self, task_id: str, value: Any):
        self._refs[task_id] = value

    def ref(self, task_id: str) -> Any:
        """
        Get the output of task with ID `task_id`

        args:
            task_id: ID of task from which to retrieve output
        returns:
            the output of the inputted `task_id`
        raises:
            prism.exception.RefDoesNotExistException if the task ID is not found
        """
        if task_id not in self._refs.keys():
            raise ValueError(f"task ID `{task_id}` not found in job `{self.run_id}`!")
        return self._refs[task_id]

    def conn(self, connector_id: str) -> Connector:
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
                f"connector ID `{connector_id}` not found job `{self.run_id}`!"
            )
        return self.connectors[connector_id]


if __name__ != "__main__":
    # Create a `CurrentRun` object. This is the object that users import within their
    # task modules. Here, we are relying on Python's import caching to ensure that the
    # refs persist across tasks. When the user creates a job, we automatically import
    # this module and create the `CurrentRun` object. Then, when users import CurrentRun
    # within their tasks, Python will not re-import and re-create a new CurrentRun
    # object. Rather, it will recognize that a CurrentRun object already exists and use
    # that for computations. When we run a task, we update the CurrentRun's `data`
    # attribute with that task's output — this allows a task's output to persist across
    # tasks.
    CurrentRun: _CurrentRun = _CurrentRun({})
