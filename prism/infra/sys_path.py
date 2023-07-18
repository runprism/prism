"""
Prism Project class

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Prism-specific imports
import site
import importlib
from prism.mixins.sys_handler import SysHandlerMixin

# Standard library imports
from pathlib import Path
from typing import Any, Dict


####################
# Class definition #
####################

class SysPathEngine(SysHandlerMixin):
    """
    Class for modifying the project's sys.path
    """

    def __init__(self,
        run_context: Dict[Any, Any],
        project_dir: Path,
    ):
        self.run_context = run_context
        self.project_dir = project_dir

        # Define base sys path and base sys tasks
        importlib.reload(site)
        import sys
        self.base_sys_path = sys.path.copy()
        self.base_sys_tasks = {
            k: v for k, v in sys.modules.items()
        }

    def modify_sys_path(self, sys_path_config):
        """
        Modify the sys.path values for this project
        """
        # Configure sys.path
        self.add_paths_to_sys_path(
            [Path(_p) for _p in sys_path_config],
            self.run_context
        )

        # Return run context
        return self.run_context

    def revert_to_base_sys_path(self, sys_path_config, run_context: Dict[Any, Any]):
        """
        Remove project dir and all associated tasks from sys path
        """
        run_context = self.remove_project_tasks(
            sys_path_config, run_context
        )
        self.run_context = self.remove_paths_from_sys_path(
            sys_path_config, run_context
        )

        # Return run context
        return self.run_context
