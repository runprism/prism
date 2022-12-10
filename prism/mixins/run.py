"""
Mixin classes for each task

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Standard library imports
import argparse
from typing import Any, Dict
from pathlib import Path

# Prism-specific imports
from prism.infra import project as prism_project
from prism.infra import pipeline as prism_pipeline
from prism.infra import executor as prism_executor


####################
# Class definition #
####################

class RunMixin():
    """
    Mixin for connect task
    """

    def create_pipeline(self,
        project: prism_project.PrismProject,
        dag_executor: prism_executor.DagExecutor,
        run_context: Dict[Any, Any]
    ) -> prism_pipeline.PrismPipeline:
        """
        Wrapper for creation of PrismPipeline object. Needed in order to be compatible
        with event manager.

        args:
            code: str or code object to run
            globals_dict: globals dictionary
        returns:
            PrismPipeline object
        """
        pipeline = prism_pipeline.PrismPipeline(
            project, dag_executor, run_context
        )
        return pipeline
