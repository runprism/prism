"""
Target decorators

Table of Contents
- Imports
- Target decorators
"""

###########
# Imports #
###########

# Standard library imports
from pathlib import Path

# Prism imports
import prism.exceptions
from prism.task import PrismTask
import prism.infra.hooks
import prism.infra.task_manager
import inspect


#####################
# Target decorators #
#####################

def target(*, type, loc, **kwargs):
    """
    Decorator to use if user wishes to save the output of a task to an external location
    (e.g., a data warehouse, an S3 bucket, or a local filepath).
    """

    def decorator_target(func):

        def wrapper_target_dec(self,
            task_manager: prism.infra.task_manager.PrismTaskManager,
            hooks: prism.infra.hooks.PrismHooks
        ):

            # This will only ever be called inside a PrismTask
            if not isinstance(self, PrismTask):
                raise prism.exceptions.RuntimeException(
                    message="`target` decorator can only be called within a Prism task"
                )

            # In cases with multiple decorators, we don't want to "chain" the
            # decorators. Rather, we want each target declaration to apply to each
            # object returned. In this case, keep track of the target types, locs, and
            # kwargs.
            if func.__name__ == "wrapper_target_dec":
                self.types.append(type)
                self.locs.append(loc)
                try:
                    self.kwargs.append(kwargs)
                except TypeError:
                    self.kwargs.append({})

                # Return the next wrapper_target function with the same arguments as
                # this one. If a function has `n` targets, then this will happen n-1
                # times until the `run` function is reached.
                if not inspect.ismethod(func):
                    return func(self, task_manager, hooks)
                else:
                    return func(task_manager, hooks)

            # Now, we've hit the `run` function
            else:

                # Confirm function name
                if func.__name__ != "run":
                    raise prism.exceptions.RuntimeException(
                        message="`target` decorator can only be called on `run` function"  # noqa: E501
                    )

                # If the task should be run in full, then call the run function
                if self.bool_run and not self.is_done:

                    # When using `target` as a decorator, `run` is a function. When
                    # using `target` as an argument to the `task()` decorator, `run` is
                    # a bound method.
                    if not inspect.ismethod(func):
                        obj = func(self, task_manager, hooks)
                    else:
                        obj = func(task_manager, hooks)
                    self.types.append(type)
                    self.locs.append(loc)
                    try:
                        self.kwargs.append(kwargs)
                    except TypeError:
                        self.kwargs.append({})

                    # If multiple things returned, we expected multiple targets
                    if isinstance(obj, tuple):
                        objects_to_save = zip(obj, self.types, self.locs, self.kwargs)
                        for zipped in objects_to_save:
                            temp_o = zipped[0]
                            temp_t = zipped[1]
                            temp_l = zipped[2]
                            temp_k = zipped[3]
                            target = temp_t(temp_o, temp_l, hooks)  # type: ignore
                            target.save(**temp_k)

                        # If a target is set, just assume that the user wants to
                        # reference the location of the target when they call `mod`
                        return obj

                    # If return type is not a Tuple, we expect a single target
                    else:

                        # Initialize an instance of the target class and save the object
                        # using the target's `save` method
                        target = type(obj, loc, hooks)
                        target.save(**kwargs)

                        # Return the object
                        return obj

                # If the task should not be run in full, then just return the location
                # of the target
                else:
                    # We still need to append the last location to self.locs
                    self.locs.append(loc)
                    self.types.append(type)

                    # If multiple targets, then return all locs
                    if len(self.locs) > 1:
                        all_objs = []
                        for _loc, _type in zip(self.locs, self.types):
                            target = _type.open(_loc, hooks)
                            all_objs.append(target.obj)
                        return tuple(all_objs)

                    # For single-target case, return single loc
                    else:
                        return self.types[0].open(self.locs[0], hooks).obj

        return wrapper_target_dec

    return decorator_target


def target_iterator(*, type, loc, **kwargs):
    """
    Decorator to use if task requires user to iterate through several different objects
    and save each object to an external location
    """

    def decorator_target_iterator(func):

        def wrapper(self,
            task_manager: prism.infra.task_manager.PrismTaskManager,
            hooks: prism.infra.hooks.PrismHooks
        ):

            # This will only ever be called inside a PrismTask
            if not isinstance(self, PrismTask):
                raise prism.exceptions.RuntimeException(
                    message="`target` decorator can only be called within a Prism task"
                )

            # Confirm function name
            if func.__name__ != "run":
                raise prism.exceptions.RuntimeException(
                    message="`target iterator` decorator can only be called on `run` function"  # noqa: E501
                )

            if self.bool_run:
                if not inspect.ismethod(func):
                    objs = func(self, task_manager, hooks)
                else:
                    objs = func(task_manager, hooks)
                if not isinstance(objs, dict):
                    raise prism.exceptions.RuntimeException(
                        message="output of run function should be dict mapping name --> object to save"  # noqa: E501
                    )
                for k, _ in objs.items():
                    if not isinstance(k, str):
                        raise prism.exceptions.RuntimeException(
                            message="output of run function should be dict mapping name --> object to save"  # noqa: E501
                        )

                # Iterate through objects and save them out
                for name, obj in objs.items():
                    target = type(obj, Path(loc) / name, hooks)
                    target.save(**kwargs)

                return loc
            else:
                return loc
        return wrapper

    return decorator_target_iterator
