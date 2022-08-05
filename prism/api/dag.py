"""
PrismDAG class definition. This class can be used to manipulate Prism projects / tasks without the CLI

Table of Contents
- Imports
- Functions / utils
- Class definition
"""


#############
## Imports ##
#############

# Standard library imports
import os
from pathlib import Path
from typing import Any, Optional, Tuple, Union

# Prism-specific imports
import prism.constants
import prism.cli.base
import prism.exceptions
from prism.infra import project as prism_project
from prism.infra import executor as prism_executor
from prism.infra import module as prism_module
import prism.mixins.compile
import prism.mixins.connect
import prism.mixins.run
import prism.mixins.sys_handler
from prism.parsers import ast_parser


######################
## Class definition ##
######################

class RunArgs:
    """
    The compile/connect CLI task only call EventManagers (which require an `args` param) within their class. However,
    the run task uses non-CLI classes that also EventManagers, such as the DAGExecutor. We need to supply an `args`
    param to the API run call, and we use this class to do so.
    """
    def __init__(self, **kwargs):
        for k,v in kwargs.items():
            self.__setattr__(k, v)
        
        # Set attributes for specific args used by non-CLI classes. Kinda messy, but we can fix later.
        if kwargs.get('full_tb') is None:
            self.full_tb = False
        if kwargs.get('quietly') is None:
            self.quietly = True

        
class PrismDAG(
    prism.mixins.compile.CompileMixin,
    prism.mixins.connect.ConnectMixin,
    prism.mixins.run.RunMixin,
    prism.mixins.sys_handler.SysHandlerMixin
):
    """
    Class to access Prism infrastructure without the CLI
    """

    def __init__(self,
        project_dir: Path,
        profiles_dir: Optional[Path] = None
    ):
        self.project_dir = project_dir
        self.profiles_dir = project_dir if profiles_dir is None else profiles_dir

        # Check if project is valid
        self._is_valid_project(self.project_dir)

        # Modules directory
        self.modules_dir = self.get_modules_dir(self.project_dir)

        # All modules in project
        self.all_modules = self.get_modules(self.modules_dir)

        # Define project namespace
        self.globals_namespace = prism.constants.GLOBALS_DICT.copy()

    
    def _is_valid_project(self, user_project_dir: Path) -> bool:
        """
        Determine if `user_project_dir` is a valid project (i.e., that is has a `prism_project.py` file and a `modules`
        folder)

        args:
            user_project_dir: project path
        raises:
            exception if Prism project is not valid
        """
        os.chdir(user_project_dir)
        temp_project_dir = prism.cli.base.get_project_dir()

        # Inputted project directory is not the same as the computed project directory
        if temp_project_dir!=user_project_dir:
            msg_list = [
                f'no project at `{str(user_project_dir)}',
                f'closest project found at `{str(temp_project_dir)}`'
            ]
            raise prism.exceptions.InvalidProjectException(message='\n'.join(msg_list))
        
        # Modules folder is not found
        if not Path(user_project_dir / 'modules').is_dir():
            raise prism.exceptions.InvalidProjectException(message=f'`modules` directory not found in `{str(user_project_dir)}`')
        
        return True

    
    def connect(self, **kwargs):
        """
        Connect task to an adapter
        """
        # Parse arguments
        connection_type = kwargs.get('type')
        if connection_type is None or not isinstance(connection_type, str):
            raise prism.exceptions.RuntimeException(message=f'connection type cannot be None')
        if connection_type not in prism.constants.VALID_ADAPTERS:
            msg_list = [
                f'invalid connection type `{connection_type}',
                f'must be one of {",".join(prism.constants.VALID_ADAPTERS)}'
            ]
            raise prism.exceptions.RuntimeException(message=f'\n'.join(msg_list))
        
        # Define profiles filepath and run
        profiles_filepath = self.profiles_dir / 'profile.yml'
        self.create_connection(connection_type, profiles_filepath)


    def compile(self, **kwargs):
        """
        Compile the Prism project
        """
        # Parse arguments
        user_arg_modules = kwargs.get('modules')
        if user_arg_modules is None:
            user_arg_modules = self.all_modules
        self.user_arg_modules = user_arg_modules
        
        # Create compiled directory and compile the DAG
        compiled_dir = self.create_compiled_dir(self.project_dir)
        self.compiled_dir = compiled_dir
        return self.compile_dag(self.project_dir, self.compiled_dir, self.all_modules, self.user_arg_modules)

    
    def run(self, **kwargs):
        """
        Run the Prism project
        """
        # Parse arguments
        env = kwargs.get('env')
        if env is None:
            env = "local"
        all_upstream = kwargs.get('all_upstream')

        # Get compiled DAG
        profiles_path = self.profiles_dir / 'profile.yml'
        compiled_dag = self.compile(**kwargs)
        compiled_dag.add_full_path(self.modules_dir)

        # Create Project, DAGExecutor, and Pipeline objects
        prism_project = self.create_project(self.project_dir, profiles_path, env, "run")
        threads = prism_project.thread_count
        dag_executor = prism_executor.DagExecutor(compiled_dag, all_upstream, threads)
        pipeline = self.create_pipeline(prism_project, dag_executor, self.globals_namespace)

        # Create args and exec
        args = RunArgs(**kwargs)
        output = pipeline.exec(args)
        if output.error_event is not None:
            event = output.error_event
            try:
                # Exception is a PrismException
                exception = event.err
            except AttributeError:
                # All other exceptions
                exception = event.value
            raise exception

    
    def _get_task_cls_from_namespace(self, module_path: Path):
        """
        Get PrismTask associated with `module_path` from Prism namespace

        args:
            module_path: path to module (relative to `modules/` folder)
        returns:
            task class
        """

        # Name of variable containing the PrismTask for `module_path`
        task_var_name = prism_module.get_task_var_name(module_path)
        task_cls = self.globals_namespace.get(task_var_name)

        # If class is None, then raise an error. Otherwise, return the class
        if task_cls is None:
            msg_list = [
                f'no output found for `{str(module_path)}` in the PrismProject namespace',
                f'check that `{str(module_path)}` has a PrismTask and that `{str(module_path)}` has been run'
            ]
            raise prism.exceptions.RuntimeException(message='\n'.join(msg_list))
        return task_cls
    

    def get_task_output(self, module_path: Path, bool_run: bool = False, **kwargs) -> Any:
        """
        Get output of the task in `module`. If bool_run==True, then run the project first. 
        Note that if bool_run==False, then only tasks with a target can be parsed.

        args:
            module: path to module (relative to `modules/` folder)
            bool_run: boolean indicating whether to run pipeline first; default is False
            **kwargs: keyword arguments for running
        returns:
            task output
        """

        # The user may have run the project in their script. Try retrieving the output
        try:
            task_cls = self._get_task_cls_from_namespace(module_path)
            return task_cls.get_output()

        except prism.exceptions.RuntimeException:
            # If project is run, then any output can be retrieved from any task
            if bool_run:
                self.run(**kwargs)
                task_cls = self._get_task_cls_from_namespace(module_path)
                return task_cls.get_output()
            
            # If project is not run, then only targets can be retrieved
            else:
                parsed_ast_module = ast_parser.AstParser(module_path, self.modules_dir)
                prism_task_cls = parsed_ast_module.get_prism_task_node(parsed_ast_module.classes, parsed_ast_module.bases)
                prism_task_cls_name = prism_task_cls.name
                task_var_name = prism_module.get_task_var_name(module_path)

                # Not sure how to instantiate the class from ast.ClassDef, so use exec.
                temp_namespace = {}
                self.add_sys_path(self.project_dir, temp_namespace)     # Add project dir to sys.path
                code_str = [
                    parsed_ast_module.module_str,
                    f'{task_var_name} = {prism_task_cls_name}(False)',  # Do NOT run the task
                    f'{task_var_name}.set_psm(None)',                   # No need for an actual psm obj, since we're only accessing the target
                    f'{task_var_name}.exec()'
                ]
                exec('\n'.join(code_str), temp_namespace)
                output = temp_namespace[task_var_name].get_output()     # This should return an error if no target is specified
                return output


# EOF