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
        parsed_models: List[ast_parser.AstParser]
    ):
        self.upstream = upstream
        self.parsed_models = parsed_models

        # Keep track of the current modules
        self.curr_module: str

    def ref(self, model: str, local: bool = False):
        # Remove the `.py`, if it exists
        model = re.sub(r'\.py$', '', model)

        try:
            # If the model is only one word, then it is either:
            #    1) a module name
            #    2) a local model
            model_split = model.split(".")
            if len(model_split) == 1:

                # Handle the case where the model is local. In this case, the model will
                # live in in current module.
                if local:
                    return self.upstream[f"{self.curr_module}.{model}"].get_output()

                # Otherwise, grab the model name from the referenced module. Note that
                # this module will definitely only contain one model (we check this when
                # parsing the refs)
                else:
                    refd_parsed_model = [
                        _p for _p in self.parsed_models if _p.model_relative_path == Path(f"{model}.py")  # noqa: E501
                    ][0]

                    # Just double-check that there is only one model
                    if len(refd_parsed_model.prism_task_nodes) > 1:
                        raise prism.exceptions.RuntimeException(
                            message=f"module `{model}` has multiple models...specify the model name and try again"  # noqa: E501
                        )
                    return self.upstream[f"{model}.{refd_parsed_model.prism_task_names[0]}"].get_output()  # noqa: E501
            else:
                return self.upstream[model].get_output()
        except KeyError:
            raise prism.exceptions.RuntimeException(
                f"could not find task `{model}`"
            )
