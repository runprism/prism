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
import re
from typing import Any, Dict


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

    def ref(self, model: str):
        if len(re.findall(r'\.py$', model)) == 0:
            model = f'{model}.py'
        return self.upstream[model].get_output()
