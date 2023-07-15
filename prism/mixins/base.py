"""
Mixin classes for base task

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Standard library imports
from typing import Any, Dict
from pathlib import Path

# Prism-specific imports
from prism.infra import project as prism_project


####################
# Class definition #
####################

class BaseMixin:
    """
    Mixin for base task
    """

    def create_project(self,
        project_dir: Path,
        user_context: Dict[str, Any],
        which: str,
        filename: str = 'prism_project.py'
    ) -> prism_project.PrismProject:
        """
        Wrapper for creation of PrismPipeline object. Needed in order to be compatible
        with event manager.

        args:
            project_dir: project directory
            user_context: user-defined context to override prism_project.py
            which: task that requires Prism project
            filename: filename of prism_project.py. Default is `prism_project.py`.
        returns:
            PrismProject object
        """
        project = prism_project.PrismProject(
            project_dir=project_dir,
            user_context=user_context,
            which=which,
            filename=filename
        )
        project.setup()
        return project
