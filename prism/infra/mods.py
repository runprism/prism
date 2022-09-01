"""
PrismHooks class

Table of Contents
- Imports
- Class definition
"""

#############
## Imports ##
#############

# Standard library imports
import pandas as pd
from typing import Any, Dict, Optional

# Prism-specific imports
from prism.infra import project as prism_project
import prism.constants
import prism.exceptions
import prism.logging


######################
## Class definition ##
######################


class PrismMods:
    """
    PrismMods class. The only purpose of this class is to store the `ref` function.
    This function allows users to reference the outputs of other tasks.
    """

    def __init__(self, upstream: Dict[str, Any]):
        self.upstream = upstream

    
    def ref(self, module: str):
        return self.upstream[module].get_output()


# EOF