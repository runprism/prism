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
from dataclasses import dataclass
from multiprocessing.dummy import Pool
from typing import Any, Dict, List, Optional
from unittest import runner

# Prism-specific imports
from prism.infra import module as prism_module
from prism.infra import compiler as prism_compiler
from prism.infra.psm import PrismFunctions
from prism.logging import Event
from prism.event_managers import base as base_event_manager


######################
## Class definition ##
######################

@dataclass
class ExecutorOutput:
    success: int
    error_event: Optional[Event]
    event_list: List[Event]


class DagExecutor:
    """
    Class for introducing concurrency in DAG execution
    """

    def __init__(self,
        compiled_dag: prism_compiler.CompiledDag,
        user_arg_all_upstream: bool,
        threads: int = 1
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
            self.dag_module_objects.append(prism_module.CompiledModule(relative, full, self.compiled_dag.manifest))
        
        # Number of processes used to run concurrent tasks
        self.threads = threads


    def set_executor_globals(self, executor_globals: Dict[Any, Any]):
        """
        Set executor globals; needs to be called before `exec`

        args:
            executor_globals: globals for DAG execution
        """
        self.executor_globals = executor_globals


    def check_task_refs(self, module: prism_module.CompiledModule) -> List[str]:
        """
        Get task refs

        args:
            module: CompiledModule object
        returns:
            refs as a list of strings
        """
        if isinstance(module.refs, str):
            return [module.refs]
        else:
            return module.refs


    def exec_single(self, args, module: prism_module.CompiledModule, psm: PrismFunctions):
        """
        Callback used to get results of module execution in Pool
        """
        # Keep track of events
        event_list: List[Event] = []
        if psm==0:
            return 0, event_list
        name = module.name
        relative_path = module.module_relative_path
        full_path = module.module_full_path

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
            func=module.exec
        )
        script_event_manager_result = script_manager.manage_events_during_run(
            event_list,
            fire_exec_events,
            globals_dict=self.executor_globals,
            psm=psm,
            explicit_run=relative_path not in self.nodes_not_explicitly_run
        )
        return script_event_manager_result

    
    def _cancel_connections(self, pool):
        """
        Given a pool, cancel all adapter connections and wait until all
        runners gentle terminates.
        """
        pool.close()
        pool.terminate()
        pool.join()
    

    def exec(self, args):
        """
        Execute DAG. Our general approach is as follows:
            1. Create a queue of the tasks (i.e., the modules) that need to be executed.
            2. Create a pool with `n` processes
            3. While queue is not empty
                - Get task from the top of the queue
                - If task doesn't have any refs, add it to the pool
                - If task does have refs, then wait for those refs to finish before adding it to pool
                - Once task has been added to queue, remove it from queue
        """

        # Create pool
        pool = Pool(processes=self.threads)
        async_results = {}

        # Keep track of events
        self.event_list: List[Event] = []
        
        def callback(result):
            psm = result.outputs
            error_event = result.event_to_fire
            runner_event_list = result.event_list

            # If psm==0, then we want to raise an error. However, if we do so here, it'll
            # get swallowed by the pool.
            if psm==0:
                self._wait_and_return = True
                self.error_event = error_event
            self.psm = psm
            self.event_list+=runner_event_list
            return

        # Execute all statements, stopping at first error
        self.psm = self.executor_globals['psm']
        self._wait_and_return = False
        self.error_event = None

        # If single-threaded, just run the modules in order
        if self.threads==1:
            while self.dag_module_objects!=[]:
                curr = self.dag_module_objects.pop(0)
                result = self.exec_single(args, curr, self.psm)
                callback(result)
                if self.psm==0:
                    return ExecutorOutput(0, self.error_event, self.event_list)
            return ExecutorOutput(1, self.error_event, self.event_list)

        # If the pool has multiple threads, then iterate through modules and add them to the Pool
        else:
            while self.dag_module_objects!=[]:
                
                # Get first module to execute
                curr = self.dag_module_objects[0]
                refs = self.check_task_refs(curr)

                # If an error occurred, skip all remaining tasks
                if self._wait_and_return:
                    break # do nothing
                
                else:

                    # If no refs, then add module to pool
                    if len(refs)==0:                
                        res = pool.apply_async(self.exec_single, args=(args,curr,self.psm), callback=callback)
                        async_results[curr.name] = res
                        self.dag_module_objects.pop(0)
                    
                    # Since DAG is run in order, refs should have been added to pool
                    # before current task. Wait for upstream tasks
                    else:
                        for ref in refs:
                            async_results[ref].wait()
                        
                        # If an error occurred, skip all remaining tasks
                        if self._wait_and_return:
                            break # do nothing
                        else:
                            res = pool.apply_async(self.exec_single, args=(args,curr,self.psm), callback=callback)
                            async_results[curr.name] = res
                            self.dag_module_objects.pop(0)
            pool.close()
            pool.join()
            
            # Wait until all tasks have finished before returning
            [res.wait() for _, res in async_results.items()]

            # If error was found, then terminate pool
            if self._wait_and_return:
                self._cancel_connections(pool)
                return ExecutorOutput(0, self.error_event, self.event_list)
            
            # Otherwise, pool should close automatically
            else:
                return ExecutorOutput(1, self.error_event, self.event_list)


    # EOF