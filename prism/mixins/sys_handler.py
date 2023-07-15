"""
Mixin classes for SysHandler

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


####################
# Class definition #
####################


class SysHandlerMixin:
    """
    Class for managing sys.path and sys.tasks
    """

    def add_paths_to_sys_path(self,
        paths: List[Path],
        globals_dict: Dict[Any, Any]
    ):
        """
        Add paths in `paths` to sys.path

        args:
            paths: paths to add
            globals_dict: sys namespace
        returns:
            none, this function updates the sys.path in-place
        """
        if 'sys' not in globals_dict.keys():
            exec('import sys', globals_dict)

        # Add the paths before the standard sys.path locations, in case there are any
        # overwrites
        paths_to_add = []
        for p in paths:
            if str(p) not in globals_dict['sys'].path:
                paths_to_add.append(str(p))
        globals_dict['sys'].path = paths_to_add + globals_dict['sys'].path
        return globals_dict

    def remove_paths_from_sys_path(self,
        base_sys_path: List[str],
        paths: List[Path],
        globals_dict: Dict[Any, Any]
    ):
        """
        Remove paths in `paths` to sys.path if they were not already in `base_sys_path`

        args:
            base_sys_path: base sys.path
            paths: paths to remove
            globals_dict: sys namespace
        returns:
            none, this function updates the sys.path in-place
        """
        if 'sys' not in globals_dict.keys():
            exec('import sys', globals_dict)

        for p in paths:
            try:
                if str(p) not in base_sys_path:
                    globals_dict['sys'].path.remove(str(p))
            except ValueError:
                continue
        return globals_dict

    def add_sys_path(self, project_dir: Path, globals_dict: Dict[Any, Any]):
        """
        Add project directory to sys.path
        """
        exec('import sys', globals_dict)

        # We only handle the sys.path stuff when defining the run context at the
        # beginning of a project run (e.g., we never call this when executing specific
        # tasks).
        globals_dict['sys'].path.insert(0, str(project_dir))
        return globals_dict

    def remove_sys_path(self, project_dir: Path, globals_dict: Dict[Any, Any]):
        """
        Remove project directory from sys.path
        """
        globals_dict['sys'].path.remove(str(project_dir))
        return globals_dict

    def remove_project_tasks(self,
        paths: List[Path],
        globals_dict: Dict[Any, Any]
    ):
        """
        Remove paths and dependent tasks in `paths` from the sys.paths and
        sys.tasks. Make sure to only remove them if they were not part of the base
        configuration. This usually isn't necessary, because prism projects run in their
        own Python session. However, there may be cases where the user runs multiple
        prism projects during the same session (e.g., during integration tests).

        This is definitely not best practice; need to find a better way of doing this.

        args:
            base_sys_tasks: base sys.tasks
            paths: custom paths (`SYS_PATH_CONF` in prism_project.py)
            globals_dict: globals dictionary
        returns:
            None
        """
        if 'sys' not in globals_dict.keys():
            return globals_dict

        # Iterate through all tasks. Only delete tasks that (1) originate from a
        # path in `paths`, and (2) did not exist in the `base_sys_tasks``
        mods_to_del = []
        for mod_name, mod_obj in globals_dict['sys'].modules.items():
            try:
                if mod_obj.__file__ is None:
                    continue
                elif any([p in paths for p in Path(mod_obj.__file__).parents]):
                    mods_to_del.append(mod_name)
            except AttributeError:
                continue

        # Delete tasks
        for mod in mods_to_del:
            del globals_dict['sys'].modules[mod]
            try:
                del globals_dict[mod]
            except KeyError:
                continue

        return globals_dict
