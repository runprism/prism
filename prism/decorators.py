"""
Target decorators

Table of Contents
- Imports
- Target decorators
"""

#############
## Imports ##
#############

# Standard library imports
from pathlib import Path
from typing import Tuple

# Prism imports
import prism.exceptions
from prism.task import PrismTask
import prism.infra.hooks
import prism.infra.task_manager


#######################
## Target decorators ##
#######################

def target(*, type, loc, **kwargs):
    """
    Decorator to use if user wishes to save the output of a task to an external location (e.g., a data warehouse, an
    S3 bucket, or a local filepath).
    """

    def decorator_target(func):

        def wrapper_target(self, task_manager: prism.infra.task_manager.PrismTaskManager, hooks: prism.infra.hooks.PrismHooks):
            
            # This will only ever be called inside a PrismTask
            if not(isinstance, self, PrismTask):
                raise prism.exceptions.RuntimeException(message="`target` decorator can only be called within a Prism task")
            
            # In cases with multiple decorators, we don't want to "chain" the decorators. Rather, we want
            # each target declaration to apply to each object returned. In this case, keep track of the target types,
            # locs, and kwargs.
            if func.__name__=="wrapper_target":
                self.types.append(type)
                self.locs.append(loc)
                try:
                    self.kwargs.append(kwargs)
                except TypeError:
                    self.kwargs.append({})
            
                # Return the next wrapper_target function with the same arguments as this one. If a function has `n`
                # targets, then this will happen n-1 times until the `run` function is reached.
                return func(self, task_manager, hooks)

            # Now, we've hit the `run` function
            else:
                
                # Confirm function name
                if func.__name__!="run":
                    raise prism.exceptions.RuntimeException(message="`target` decorator can only be called on `run` function")

                # If the task should be run in full, then call the run function
                if self.bool_run:
                    obj = func(self, task_manager, hooks)
                    self.types.append(type)
                    self.locs.append(loc)
                    try:
                        self.kwargs.append(kwargs)
                    except TypeError:
                        self.kwargs.append({})

                    # If multiple things returned, we expected multiple targets
                    if isinstance(obj, Tuple):
                        objects_to_save = zip(obj, self.types, self.locs, self.kwargs)
                        for zipped in objects_to_save:
                            o = zipped[0]
                            t = zipped[1]
                            l = zipped[2]
                            k = zipped[3]
                            target = t(o, l, hooks)
                            target.save(**k)

                        # If a target is set, just assume that the user wants to reference the location of the target
                        # when they call `mod`
                        return self.locs
                    
                    # If return type is not a Tuple, we expect a single target
                    else:

                        # Initialize an instance of the target class and save the object using the target's `save` method
                        target = type(obj, loc, hooks)
                        target.save(**kwargs)

                        # If a target is set, just assume that the user wants to reference the location of the target
                        # when they call `mod`
                        return loc

                # If the task should not be run in full, then just return the location of the target
                else:
                    # If multiple targets, then return all locs
                    if len(self.locs)>0:
                        return self.locs
                    
                    # For single-target case, return single loc
                    else:
                        return loc
        return wrapper_target
    
    return decorator_target


def target_iterator(*, type, loc, **kwargs):
    """
    Decorator to use if task requires user to iterate through several different objects and save each object
    to an external location
    """

    def decorator_target_iterator(func):

        def wrapper(self, task_manager: prism.infra.task_manager.PrismTaskManager, hooks: prism.infra.hooks.PrismHooks):
            
            # This will only ever be called inside a PrismTask
            if not(isinstance, self, PrismTask):
                raise prism.exceptions.RuntimeException(message="`target` decorator can only be called within a Prism task")
        
            # Confirm function name
            if func.__name__!="run":
                raise prism.exceptions.RuntimeException(message="`target iterator` decorator can only be called on `run` function")

            if self.bool_run:
                objs = func(self, task_manager, hooks)
                if not isinstance(objs, dict):
                    msg_list = [
                        f"output of run function should be dict",
                        'mapping name --> object to save'
                    ]
                    raise prism.exceptions.RuntimeException(message='\n'.join(msg_list))
                for k,_ in objs.items():
                    if not isinstance(k, str):
                        msg_list = [
                            f"output of run function should be dict",
                            'mapping name --> object to save'
                        ]
                        raise prism.exceptions.RuntimeException(message='\n'.join(msg_list))
                
                # Iterate through objects and save them out
                for name, obj in objs.items():
                    target = type(obj, Path(loc) / name, hooks)
                    target.save(**kwargs)
                
                return loc
            else:
                return loc
        return wrapper

    return decorator_target_iterator


# EOF