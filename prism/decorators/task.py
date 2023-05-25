"""
Task decorator

Table of Contents
- Imports
- Target decorators
"""

###########
# Imports #
###########

# Standard library imports
from typing import Optional
import inspect
from functools import reduce

# Prism imports
import prism.infra.hooks
import prism.infra.task_manager
from prism.task import PrismTask


##################
# Task decorator #
##################

def bind(instance, func, as_name=None):
    """
    Bind the function *func* to *instance*, with either provided name *as_name*
    or the existing name of *func*. The provided *func* should accept the
    instance as the first argument, i.e. "self".
    """
    if as_name is None:
        as_name = func.__name__
    bound_method = func.__get__(instance, instance.__class__)
    setattr(instance, as_name, bound_method)
    return bound_method


def check_function_arguments(func):
    """
    Check function arguments and confirm that it has only `tasks` and `hooks`
    """
    sig = inspect.signature(func)
    flag_has_tasks = False
    flag_has_hooks = False
    for param in sig.parameters:
        if param == "tasks":
            flag_has_tasks = True
        if param == "hooks":
            flag_has_hooks = True
        if param not in ["tasks", "hooks"]:
            raise prism.exceptions.RuntimeException(
                f"`{func.__name__}` has unrecognized parameter `{param}`!"
            )
    if not flag_has_tasks:
        raise prism.exceptions.RuntimeException(
            f"`{func.__name__}` does not have `tasks` parameter!"
        )
    if not flag_has_hooks:
        raise prism.exceptions.RuntimeException(
            f"`{func.__name__}` does not have `hooks` parameter!"
        )


def task(*,
    retries: int = 0,
    retry_delay_seconds: Optional[int] = None,
    targets=None,
):
    """
    Decorator used to turn any Python function into a Prism task.
    """
    def decorator_task(func):

        def wrapper_task(
            task_manager: prism.infra.task_manager.PrismTaskManager,
            hooks: prism.infra.hooks.PrismHooks
        ):
            # Check function arguments
            check_function_arguments(func)

            # Create the new task. Just set bool_run to `True` for now.
            new_task = PrismTask(bool_run=True, func=func)

            # Set task manager and hooks
            new_task.task_manager = task_manager
            new_task.hooks = hooks

            # Set class attributes
            if retries:
                new_task.RETRIES = retries
            if retry_delay_seconds:
                new_task.RETRY_DELAY_SECONDS = retry_delay_seconds

            # Chain the decorators together and bind the decorated function to the task
            # instance.
            if targets:
                if len(targets) == 0:
                    pass
                decorated_func = reduce(
                    lambda x, y: y(x), reversed(targets), new_task.run  # type: ignore
                )
                new_task.run = bind(new_task, decorated_func)  # type: ignore

            return new_task

        return wrapper_task
    return decorator_task
