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
import site
import importlib


####################
# Class definition #
####################


class SysHandlerMixin:
    """
    Class for managing sys.path and sys.tasks
    """

    def import_sys(self, context: Dict[Any, Any]):
        if 'sys' not in context.keys():
            importlib.reload(site)
            context["sys"] = importlib.import_module("sys")
        return context

    def add_paths_to_sys_path(self,
        paths: List[Path],
        context: Dict[Any, Any]
    ):
        """
        Add paths in `paths` to sys.path

        args:
            paths: paths to add
            context: sys namespace
        returns:
            none, this function updates the sys.path in-place
        """
        context = self.import_sys(context)

        # Add the paths before the standard sys.path locations, in case there are any
        # overwrites
        paths_to_add = []
        for p in paths:
            if str(p) not in context['sys'].path:
                paths_to_add.append(str(p))
        context['sys'].path = paths_to_add + context['sys'].path
        return context

    def remove_paths_from_sys_path(self,
        paths: List[Path],
        context: Dict[Any, Any]
    ):
        """
        Remove paths in `paths` to sys.path if they were not already in `base_sys_path`

        args:
            base_sys_path: base sys.path
            paths: paths to remove
            context: sys namespace
        returns:
            none, this function updates the sys.path in-place
        """
        context = self.import_sys(context)

        for p in paths:
            try:
                context['sys'].path.remove(str(p))
            except ValueError:
                continue
        return context

    def add_sys_path(self, project_dir: Path, context: Dict[Any, Any]):
        """
        Add project directory to sys.path
        """
        context = self.import_sys(context)

        # We only handle the sys.path stuff when defining the run context at the
        # beginning of a project run (e.g., we never call this when executing specific
        # tasks).
        context['sys'].path.insert(0, str(project_dir))
        return context

    def remove_sys_path(self, project_dir: Path, context: Dict[Any, Any]):
        """
        Remove project directory from sys.path
        """
        context['sys'].path.remove(str(project_dir))
        return context

    def remove_project_tasks(self,
        paths: List[Path],
        context: Dict[Any, Any]
    ):
        """
        Remove paths and dependent tasks in `paths` from the sys.paths and
        sys.tasks. Make sure to only remove them if they were not part of the base
        configuration. This usually isn't necessary, because prism projects run in their
        own Python session. However, there may be cases where the user runs multiple
        prism projects during the same session (e.g., during integration tests).

        This is definitely not best practice; need to find a better way of doing this.

        args:
            paths: custom paths (`SYS_PATH_CONF` in prism_project.py)
            context: globals dictionary
        returns:
            None
        """
        if 'sys' not in context.keys():
            return context

        # Iterate through all tasks. Only delete tasks that (1) originate from a
        # path in `paths`, and (2) did not exist in the `base_sys_tasks``
        mods_to_del = []
        for mod_name, mod_obj in context['sys'].modules.items():
            try:
                if mod_obj.__file__ is None:
                    continue
                elif any([p in paths for p in Path(mod_obj.__file__).parents]):
                    mods_to_del.append(mod_name)
            except AttributeError:
                continue

        # Delete tasks
        for mod in mods_to_del:
            del context['sys'].modules[mod]
            try:
                del context[mod]
            except KeyError:
                continue

        return context
