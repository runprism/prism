"""
PrismTask class definition. All user-created tasks should inherit from PrismTask.

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Standard library imports
from pathlib import Path
from typing import Any, Callable, List, Optional, Union

# Prism imports
import prism.exceptions
import prism.target


####################
# Class definition #
####################


class PrismTask:
    retries: int
    retry_delay_seconds: int

    def __init__(
        self,
        task_id: str,
        func: Optional[Callable[..., Any]] = None,
        bool_run: bool = True,
    ):
        """
        Create an instance of the PrismTask. The class immediately calls the `run`
        function and assigns the result to the `output` attribute.
        """
        self.task_id = task_id
        self.func = func
        self.bool_run = bool_run

        # Tyeps, locs, and kwargs for target
        self.types: List[prism.target.PrismTarget] = []
        self.locs: List[Union[str, Path]] = []
        self.kwargs: List[Any] = []

        # Retries
        self.retries = 0
        self.retry_delay_seconds = 0

        # Initialize the is_done attribute
        self.is_done: bool = False

    def exec(self):

        # If the `target` decorator isn't applied, then only execute the `run` function
        # of bool_run is true
        if self.run.__name__ == "run" and not self.is_done:

            # If bool_run, then execute the `run` function and set the `output`
            # attribute to its result
            if self.bool_run:
                self.output = self.run()
                if self.output is None:
                    raise prism.exceptions.RuntimeException(
                        "`run` method must produce a non-null output"
                    )

            # If the code reaches this stage, then the user is attempting to use this
            # tasks output without explicitly running the task or setting a target. We
            # will throw an error in the get_output() method.
            else:
                self.output = None

        # Otherwise, the decorator uses bool_run in its internal computation
        else:
            self.output = self.run()
            if self.output is None:
                raise prism.exceptions.RuntimeException(
                    "`run` method must produce a non-null output"
                )

    def done(self) -> bool:
        return False

    def run(self):
        if self.func is not None:
            return self.func()
        else:
            raise prism.exceptions.RuntimeException("`run` method not implemented")

    def get_output(self):
        """
        Return the output attribute
        """
        # If self.output is None, then the user has not specified a target nor have they
        # explicitly run the task.
        if self.output is None:
            msg = f"cannot access the output of `{self.__class__.__name__}` without either explicitly running task or setting a target"  # noqa: E501
            raise prism.exceptions.RuntimeException(message=msg)
        return self.output
