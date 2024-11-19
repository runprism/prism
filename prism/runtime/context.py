from typing import Any, Dict, Optional

# Prism-specific imports
from prism.connectors.base import Connector


class _Context:
    """
    THIS CLASS SHOULD NEVER BE IMPORTED DIRECTLY. INSTEAD, IMPORT `Context`, i.e.,
    ```python
        from prism.runtime import Context
    ```

    Context stores contextual variables for that tasks can access during runtime.
    This includes variables that the user specifies in their client definition and
    connectors.
    """

    run_id: str
    _data: Dict[str, Any]
    connectors: Dict[str, Connector]

    def __init__(self):
        pass

    def _setup(
        self,
        run_id: str,
        runtime_ctx: Dict[str, Any],
    ):
        self.run_id = run_id
        self._data = runtime_ctx

    def __call__(self, key: str, default_value: Optional[Any] = None) -> Any:
        """
        Get the value associated with context variable `key`. Context variables can be
        set in two places: when instantiated the PrismProject (with the `ctx` keyword
        argument) and when creating the run (with the `runtime_ctx` keyword argument
        in the PrismProject's `run` method).

        args:
            key: variable to retrieve
            default_value: default value to return if `key` is not found.
                Default is `None`
        returns:
            value associated with context variable `key`
        """
        return self._data.get(key, default_value)


if __name__ != "__main__":
    Context: _Context = _Context()
