from functools import reduce
from typing import Optional

# Prism imports
from prism.task import PrismTask


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


def task(
    *,
    task_id: Optional[str] = None,
    retries: int = 0,
    retry_delay_seconds: Optional[int] = None,
    targets=None,
):
    """
    Decorator used to turn any Python function into a Prism task.
    """

    def decorator_task(func):
        def wrapper_task(task_id: Optional[str] = task_id, bool_run: bool = True):
            assert task_id
            new_task = PrismTask(task_id=task_id, func=func, bool_run=bool_run)

            # Set class attributes
            if retries:
                new_task.retries = retries
            if retry_delay_seconds:
                new_task.retry_delay_seconds = retry_delay_seconds

            # Chain the decorators together and bind the decorated function to the task
            # instance.
            if targets:
                if len(targets) == 0:
                    pass
                decorated_func = reduce(
                    lambda x, y: y(x),
                    reversed(targets),
                    new_task.run,  # type: ignore
                )
                new_task.run = bind(new_task, decorated_func)  # type: ignore

            return new_task

        return wrapper_task

    return decorator_task
