"""
Metaclass for agent. This allows to create instances of adapter classes using strings.

Table of Contents
- Class definition
"""

from typing import Any, Dict


####################
# Class definition #
####################

class MetaAgent(type):

    classes: Dict[Any, Any] = {}

    def __new__(cls, name, bases, dct):
        result = super().__new__(cls, name, bases, dct)
        cls.classes[name.lower()] = result
        return result

    @classmethod
    def get_agent(cls, name):
        return cls.classes.get(name)
