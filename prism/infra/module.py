"""
Prism Module class

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
import prism.exceptions
from prism.infra.psm import PrismFunctions
from prism.parsers.ast_parser import AstParser


######################
## Class definition ##
######################

class CompiledModule:
    """
    Class for defining and executing a single compiled module
    """

    def __init__(self, module_relative_path: Path, module_full_path: Path):
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
    

    def instantiate_module_class(self,
        globals_dict: Dict[Any, Any],
        psm: PrismFunctions,
        explicit_run: bool = True
    ):
        """
        Instantiate PrismTask child from module

        args:
            globals_dict: globals dictionary
            upstream_dict: dictionary with upstream task information
        returns:
            variable used to store task instantiation
        """
        # Get prism class from module
        prism_task_class = self.ast_module.get_prism_task_node(self.ast_module.classes, self.ast_module.bases)
        if prism_task_class is None:
            raise prism.exceptions.ParserException(message=f"no PrismTask in `{str(self.module_relative_path)}`")
        prism_task_class_name = prism_task_class.name

        # Variable name should just be the name of the module itself. A project shouldn't contain duplicate modules.
        task_var_name = self.name.replace('/', '_').replace('.py', '')

        # Execute class definition and create task
        exec(self.module_str, globals_dict)
        exec(f'{task_var_name}={prism_task_class_name}({explicit_run})', globals_dict)

        # Set PrismFunctions object
        globals_dict[task_var_name].set_psm(psm)
        
        # Return name of variable used to store task instantiation
        return task_var_name


    def exec(self,
        globals_dict: Dict[Any, Any],
        psm: PrismFunctions,
        explicit_run: bool = True
    ) -> PrismFunctions:
        task_var_name = self.instantiate_module_class(globals_dict, psm, explicit_run)
        globals_dict[task_var_name].exec()
        psm.upstream[self.name] = globals_dict[task_var_name]
        return psm


# EOF