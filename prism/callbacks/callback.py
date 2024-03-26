import importlib
import inspect
from typing import Any, Callable


class _PrismCallback:
    """
    Callbacks allow users to call specific functions when a run successfully executes or
    when a project fails. This is a super thin wrapper around the actual callback
    function.
    """

    callback_func: Callable[[], Any]
    name: str

    def __init__(self, callback_func: Callable[[], Any]):
        self.callback_func = callback_func
        self.name = callback_func.__name__

        # Check if the argument as any args. If it does, raise an error. For now, users
        # cannot specify arguments in a callback function. They can access information
        # about the run that triggered the callback via the `CurrentRun` object.
        signature = inspect.signature(self.callback_func)
        args = list(signature.parameters.keys())
        if len(args) > 0:
            raise ValueError(
                f"Callback function `{self.name}` cannot have any arguments.",
            )

    @classmethod
    def from_str(cls, import_path: str):
        module_name = ".".join(import_path.split(".")[:-1])
        fn_name = import_path.split(".")[-1]
        imported_mod = importlib.import_module(module_name)
        fn = getattr(imported_mod, fn_name)
        return cls(fn)

    def run(self):
        self.callback_func()
