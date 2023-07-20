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
from typing import Any, Dict, List
from pathlib import Path
import re


# Prism
import prism.exceptions
from prism.parsers import ast_parser


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

    def __init__(self,
        upstream: Dict[str, Any],
        parsed_tasks: List[ast_parser.AstParser]
    ):
        self.upstream = upstream
        self.parsed_tasks = parsed_tasks

        # Keep track of the current modules
        self.curr_module: str

    def ref(self, task: str, local: bool = False):
        # Remove the `.py`, if it exists
        task = re.sub(r'\.py$', '', task)

        try:
            # If the task is only one word, then it is either:
            #    1) a module name
            #    2) a local task
            task_split = task.split(".")
            if len(task_split) == 1:

                # Handle the case where the task is local. In this case, the task will
                # live in in current module.
                if local:
                    return self.upstream[f"{self.curr_module}.{task}"].get_output()

                # Otherwise, grab the task name from the referenced module. Note that
                # this module will definitely only contain one task (we check this when
                # parsing the refs)
                else:
                    refd_parsed_task = [
                        _p for _p in self.parsed_tasks if _p.task_relative_path == Path(f"{task}.py")  # noqa: E501
                    ][0]

                    # Just double-check that there is only one task
                    if len(refd_parsed_task.prism_task_nodes) > 1:
                        raise prism.exceptions.RuntimeException(
                            message=f"module `{task}` has multiple tasks...specify the task name and try again"  # noqa: E501
                        )
                    return self.upstream[f"{task}.{refd_parsed_task.prism_task_names[0]}"].get_output()  # noqa: E501
            else:
                return self.upstream[task].get_output()

        except KeyError:
            raise prism.exceptions.RuntimeException(
                f"could not find task `{task}`"
            )
