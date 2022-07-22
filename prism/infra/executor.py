"""
Prism DagExecutor class

Table of Contents
- Imports
- Class definition
"""

#############
## Imports ##
#############

# Standard library imports
from typing import Any, Dict, List

# Prism-specific imports
from prism.infra import module as prism_module
from prism.infra import compiler as prism_compiler
from prism.logging import Event
from prism.event_managers import base as base_event_manager


######################
## Class definition ##
######################

class DagExecutor:
    """
    Class for executing DAG in topological order
    """

    def __init__(self,
        compiled_dag: prism_compiler.CompiledDag,
        user_arg_all_upstream: bool
    ):
        self.compiled_dag = compiled_dag
        self.topological_sort_relative_path = self.compiled_dag.topological_sort
        self.topological_sort_full_path = self.compiled_dag.topological_sort_full_path
        self.user_arg_modules = self.compiled_dag.user_arg_modules
        self.user_arg_all_upstream = user_arg_all_upstream

        # Identify nodes not explicitly run and update (only if --all-upstream is False)
        if not self.user_arg_all_upstream:
            self.nodes_not_explicitly_run = list(set(self.topological_sort_relative_path)-set(self.user_arg_modules))
        else:
            self.nodes_not_explicitly_run = []

        # Create module objects
        self.dag_module_objects = []
        for relative, full in zip(self.topological_sort_relative_path, self.topological_sort_full_path):
            self.dag_module_objects.append(prism_module.CompiledModule(relative, full))
    

    def set_executor_globals(self, executor_globals: Dict[Any, Any]):
        """
        Set executor globals; needs to be called before `exec`

        args:
            executor_globals: globals for DAG execution
        """
        self.executor_globals = executor_globals
    

    def exec(self, args):
        """
        Execute DAG
        """
        # Keep track of events
        event_list: List[Event] = []

        # Execute all statements, stopping at first error
        psm = self.executor_globals['psm']
        for script in self.dag_module_objects:
            if psm==0:
                return 0, event_list
            
            name = script.name
            relative_path = script.module_relative_path
            full_path = script.module_full_path

            # Boolean for whether to fire exec event for current script. We do not want to fire the exec events if the
            # user did not explicitly include the script in their arguments
            fire_exec_events = relative_path in self.user_arg_modules or self.user_arg_all_upstream

            # If all upstream modules are to be run, the compute the idx and total using the `dag`` list. Otherwise,
            # if the script is explicitly included in the user's run command then compute the idx and total using
            # the `modules` list. Otherwise, set both to None.

            # First, sort the user arg modules in the order in which they appear in the DAG
            modules_idx = [self.topological_sort_relative_path.index(m) for m in self.user_arg_modules]
            modules_sorted = [x for _,x in sorted(zip(modules_idx, self.user_arg_modules))]

            # Define the idx and total
            if self.user_arg_all_upstream:
                idx = self.topological_sort_full_path.index(full_path)+1
                total = len(self.topological_sort_full_path)
            elif relative_path in self.user_arg_modules:
                idx = modules_sorted.index(relative_path)+1
                total = len(modules_sorted)
            else:
                idx = None
                total = None

            # Event manager. We want '__file__' to be the path to the un-compiled module. Instances of DagExecutor will
            # only be called within the project directory. Therefore, __files__ should be modules/{name of script}
            self.executor_globals['__file__'] = f'modules/{str(relative_path)}'
            script_manager = base_event_manager.BaseEventManager(
                args=args,
                idx=idx,
                total=total,
                name=name,
                func=script.exec
            )
            psm, event_list = script_manager.manage_events_during_run(
                event_list,
                fire_exec_events,
                globals_dict=self.executor_globals,
                psm=psm,
                explicit_run=relative_path not in self.nodes_not_explicitly_run
            )
        
        # Last script had an error
        if psm==0:
            return 0, event_list
        else:
            return 1, event_list


# EOF