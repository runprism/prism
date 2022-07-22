"""
SysHander class

Table of Contents
- Imports
- Class definition
"""

#############
## Imports ##
#############

# Standard library imports
from pathlib import Path
from typing import Any, Dict

# Prism-specific imports
from prism.infra import project as prism_project


######################
## Class definition ##
######################


class SysHandler:
    """
    Class for managing sys.path and sys.modules
    """

    def __init__(self, project: prism_project.PrismProject):
        self.project = project
        self.project_dir = self.project.project_dir


    def add_sys_path(self, globals_dict: Dict[Any, Any]):
        """
        Add project directory to sys.path
        """
        exec('import sys', globals_dict)
        globals_dict['sys'].path.insert(0, str(self.project_dir))
        return globals_dict
    
    
    def remove_sys_path(self, globals_dict: Dict[Any, Any]):
        """
        Remove project directory from sys.path
        """
        globals_dict['sys'].path.remove(str(self.project_dir))
        return globals_dict

    
    def remove_project_modules(self, globals_dict: Dict[Any, Any]):
        """
        Remove modules contained within project directory from sys.modules. This usually isn't necessary, because prism
        projects run in their own Python session. However, there may be cases where the user runs multiple prism
        projects during the same session (e.g., during integration tests).

        This is definitely not best practice; need to find a better way of doing this.
        """
        if 'sys' not in globals_dict.keys():
            return globals_dict
        
        mods_to_del = []
        for mod_name, mod_obj in globals_dict['sys'].modules.items():
            try:
                if mod_obj.__file__ is None:
                    pass
                elif self.project_dir in Path(mod_obj.__file__).parents:
                    mods_to_del.append(mod_name)
            except AttributeError:
                pass
        
        # Delete modules
        for mod in mods_to_del:
            del globals_dict['sys'].modules[mod]
        return globals_dict


# EOF