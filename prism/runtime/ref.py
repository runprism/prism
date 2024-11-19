from typing import Any, Dict


class _Ref:
    """
    THIS CLASS SHOULD NEVER BE IMPORTED DIRECTLY. INSTEAD, IMPORT `Ref`, i.e.,
    ```python
        from prism.runtime import Ref
    ```

    Ref stores the results of tasks that can be accessed at runtime.
    """

    run_id: str
    _refs: Dict[str, Any]

    def __init__(self):
        pass

    def __call__(self, task_id: str) -> Any:
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
            raise ValueError(f"task ID `{task_id}` not found in run `{self.run_id}`!")
        return self._refs[task_id]

    def _setup(self, run_id: str, ref_data: Dict[str, Any] | None = None):
        self.run_id = run_id
        self._refs = ref_data if ref_data else {}

    def _set_task_output_value(self, task_id: str, value: Any):
        self._refs[task_id] = value


if __name__ != "__main__":
    Ref: _Ref = _Ref()
