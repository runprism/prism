"""
PrismTask class definition. All user-created tasks should inherit from PrismTask.

Table of Contents
- Imports
- Class definition
"""

#############
## Imports ##
#############

import functools
import prism.exceptions
from typing import Tuple
from pathlib import Path

# Prism logging
import prism.logging
import prism.infra.hooks
import prism.infra.mods


######################
## Class definition ##
######################

class PrismTask:

    def __init__(self, bool_run=True):
        """
        Create an instance of the PrismTask. The class immediately calls the `run` function and assigns the result to
        the `output` attribute.
        """
        self.bool_run = bool_run

        # Tyeps, locs, and kwargs for target
        self.types = []
        self.locs = []
        self.kwargs = []
    
    
    def set_psm(self, psm):
        self.psm = psm

    
    def set_mods(self, mods: prism.infra.mods.PrismMods):
        self.mods = mods

    
    def set_hooks(self, hooks: prism.infra.hooks.PrismHooks):
        self.hooks = hooks

    
    def exec(self):
        
        # If the `target` decorator isn't applied, then only execute the `run` function of bool_run is true
        if self.run.__name__=="run":
            
            # If bool_run, then execute the `run` function and set the `output` attribute to its result
            if self.bool_run:
                self.output = self.run(self.mods, self.hooks)
                if self.output is None:
                    raise prism.exceptions.RuntimeException("`run` method must produce a non-null output")

            # If the code reaches this stage, then the user is attempting to use this tasks output without
            # explicitly running the task or setting a target. We will throw an error in the get_output() method.
            else:
                self.output = None
        
        # Otherwise, the decorator uses bool_run in its internal computation
        else:
            self.output = self.run(self.mods, self.hooks)
            if self.output is None:
                raise prism.exceptions.RuntimeException("`run` method must produce a non-null output")


    def run(self, psm, hooks: prism.infra.hooks.PrismHooks):
        """
        Run the task. The user should override this function definition when creating their own tasks.
        """
        raise prism.exceptions.RuntimeException("`run` method not implemented")


    @prism.logging.deprecated('PrismTask.target', 'prism.decorators.target')
    def target(type, loc, **kwargs):
        """
        Decorator to use if task requires user to iterate through several different objects and save each object
        to an external location
        """

        def decorator_target(func):
            
            def wrapper_target(self, mods: prism.infra.mods.PrismMods, hooks: prism.infra.hooks.PrismHooks):
                
                # Decorator should only be called on the `run` function
                if func.__name__!="run":
                    raise prism.exceptions.RuntimeException(message="`target` decorator can only be called on `run` function")

                # If the task should be run in full, then call the run function
                if self.bool_run:
                    obj = func(self, mods, hooks)

                    # Initialize an instance of the target class and save the object using the target's `save` method
                    target = type(obj, loc)
                    target.save(**kwargs)

                    # If a target is set, just assume that the user wants to reference the location of the target
                    # when they call `mod`
                    return loc
                
                # If the task should not be run in full, then just return the location of the target
                else:
                    return loc
            return wrapper_target
        return decorator_target
        
    
    def get_output(self):
        """
        Return the output attribute
        """
        # If self.output is None, then the user has not specified a target nor have they explicitly run the task.
        if self.output is None:
            msg_list = [
                f"cannot access the output of `{self.__class__.__name__}` without",
                "either explicitly running task or setting a target"
            ]
            raise prism.exceptions.RuntimeException(message = '\n'.join(msg_list))
        return self.output


# EOF