"""
PrismTaskManager class

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Standard library imports
from typing import Any, Dict

# Prism
import prism.exceptions


####################
# Class definition #
####################

class PrismTaskManager:
    """
    PrismTaskManager class. This class manages all tasks in the DAG (and their
    associated outputs). An instance of the task manager is passed to all `run`
    functions (via the kw `tasks`), and users can reference the output of other tasks
    via `tasks.ref('...')`.
    """

    def __init__(self, upstream: Dict[str, Any]):
        self.upstream = upstream

    def ref(self, model: str, local: bool = False):
        try:
            return self.upstream[model].get_output()
        except KeyError:
            raise prism.exceptions.RuntimeException(
                f"could not find task `{model}`"
            )
