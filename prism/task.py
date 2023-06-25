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
import prism.prism_logging
import prism.infra.hooks
import prism.infra.task_manager
import prism.target


####################
# Class definition #
####################

class PrismTask:

    def __init__(self,
        bool_run: bool = True,
        func: Optional[Callable[..., Any]] = None
    ):
        """
        Create an instance of the PrismTask. The class immediately calls the `run`
        function and assigns the result to the `output` attribute.
        """
        self.bool_run = bool_run
        self.func = func

        # Tyeps, locs, and kwargs for target
        self.types: List[prism.target.PrismTarget] = []
        self.locs: List[Union[str, Path]] = []
        self.kwargs: List[Any] = []

        # Retries
        self.RETRIES = 0
        self.RETRY_DELAY_SECONDS = 0

    def set_task_manager(self, task_manager: prism.infra.task_manager.PrismTaskManager):
        self.task_manager = task_manager

    def set_hooks(self, hooks: prism.infra.hooks.PrismHooks):
        self.hooks = hooks

    def exec(self):

        # If the `target` decorator isn't applied, then only execute the `run` function
        # of bool_run is true
        if self.run.__name__ == "run":

            # If bool_run, then execute the `run` function and set the `output`
            # attribute to its result
            if self.bool_run:
                self.output = self.run(self.task_manager, self.hooks)
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
            self.output = self.run(self.task_manager, self.hooks)
            if self.output is None:
                raise prism.exceptions.RuntimeException(
                    "`run` method must produce a non-null output"
                )

    def run(self,
        tasks: prism.infra.task_manager.PrismTaskManager,
        hooks: prism.infra.hooks.PrismHooks,
    ):
        """
        Run the task. The user should override this function definition when creating
        their own tasks.
        """
        if self.func is not None:
            return self.func(tasks, hooks)
        else:
            raise prism.exceptions.RuntimeException("`run` method not implemented")

    @prism.prism_logging.deprecated('prism.task.PrismTask.target', 'prism.decorators.target')
    def target(type, loc, **kwargs):
        """
        Decorator to use if task requires user to iterate through several different
        objects and save each object to an external location
        """

        def decorator_target(func):

            def wrapper_target(self,
                task_manager: prism.infra.task_manager.PrismTaskManager,
                hooks: prism.infra.hooks.PrismHooks
            ):

                # Decorator should only be called on the `run` function
                if func.__name__ != "run":
                    raise prism.exceptions.RuntimeException(
                        message="`target` decorator can only be called on `run` function"  # noqa: E501
                    )

                # If the task should be run in full, then call the run function
                if self.bool_run:
                    obj = func(self, task_manager, hooks)

                    # Initialize an instance of the target class and save the object
                    # using the target's `save` method
                    target = type(obj, loc, hooks=None)  # type: ignore
                    target.save(**kwargs)

                    # If a target is set, just assume that the user wants to reference
                    # the location of the target when they call `mod`
                    return loc

                # If the task should not be run in full, then just return the location
                # of the target
                else:
                    return loc
            return wrapper_target
        return decorator_target

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
