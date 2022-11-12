"""
Prism Module class

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Standard library imports
from pathlib import Path
from typing import Any, Dict

# Prism-specific imports
import prism.exceptions
from prism.infra.task_manager import PrismTaskManager
from prism.infra.hooks import PrismHooks
from prism.infra.manifest import ModuleManifest
from prism.parsers.ast_parser import AstParser


#####################
# Functions / utils #
#####################

def get_task_var_name(module_path: Path) -> str:
    """
    Retrieve the variable used to store the PrismTask in `module_path` in our namespace

    args:
        module_path: path to module, relative to `modules/`
    returns:
        variable name
    """
    task_var_name = str(module_path).replace('/', '_').replace('.py', '')
    return task_var_name


####################
# Class definition #
####################

class CompiledModule:
    """
    Class for defining and executing a single compiled module
    """

    def __init__(self,
        module_relative_path: Path,
        module_full_path: Path,
        module_manifest: ModuleManifest
    ):
        self.module_relative_path = module_relative_path
        self.module_full_path = module_full_path
        with open(self.module_full_path, 'r') as f:
            self.module_str = f.read()
        f.close()

        # Module as an AST
        parent_path = Path(str(module_full_path).replace(str(module_relative_path), ''))
        self.ast_module = AstParser(self.module_relative_path, parent_path)

        # Module name
        self.name = str(self.module_relative_path)

        # Set manifest
        self.module_manifest = module_manifest
        self.refs = self._check_manifest(self.module_manifest)

    def _check_manifest(self, module_manifest: ModuleManifest):
        """
        Check manifest and return list of refs associated with compiled
        module
        """
        refs = []
        manifest_refs = module_manifest.manifest_dict["refs"]
        for ref_obj in manifest_refs:
            refs.append(ref_obj["source"])
        if len(refs) == 1:
            refs = refs[0]
        return refs

    def instantiate_module_class(self,
        globals_dict: Dict[Any, Any],
        task_manager: PrismTaskManager,
        hooks: PrismHooks,
        explicit_run: bool = True
    ):
        """
        Instantiate PrismTask child from module

        args:
            globals_dict: globals dictionary
            task_manager: PrismTaskManager object
            hooks: PrismHooks object
            explicit run: boolean indicating whether to run the Task. Default is True
        returns:
            variable used to store task instantiation
        """
        # Get prism class from module
        prism_task_class = self.ast_module.get_prism_task_node(
            self.ast_module.classes, self.ast_module.bases
        )
        if prism_task_class is None:
            raise prism.exceptions.ParserException(
                message=f"no PrismTask in `{str(self.module_relative_path)}`"
            )
        prism_task_class_name = prism_task_class.name

        # Variable name should just be the name of the module itself. A project
        # shouldn't contain duplicate modules.
        task_var_name = get_task_var_name(self.module_relative_path)

        # Execute class definition and create task
        exec(self.module_str, globals_dict)
        exec(f'{task_var_name}={prism_task_class_name}({explicit_run})', globals_dict)

        # Set task manager and hooks
        globals_dict[task_var_name].set_task_manager(task_manager)
        globals_dict[task_var_name].set_hooks(hooks)

        # Return name of variable used to store task instantiation
        return task_var_name

    def exec(self,
        globals_dict: Dict[Any, Any],
        task_manager: PrismTaskManager,
        hooks: PrismHooks,
        explicit_run: bool = True
    ) -> PrismTaskManager:
        """
        Execute module
        """
        task_var_name = self.instantiate_module_class(
            globals_dict, task_manager, hooks, explicit_run
        )
        globals_dict[task_var_name].exec()
        task_manager.upstream[self.name] = globals_dict[task_var_name]
        return task_manager
