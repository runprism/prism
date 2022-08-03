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
from typing import List, Optional, Tuple, Union

# Prism-specific imports
import prism.constants
import prism.cli.base
import prism.exceptions
from prism.infra import project as prism_project
from prism.infra import executor as prism_executor
import prism.mixins.compile
import prism.mixins.connect
import prism.mixins.run


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
        
        # Set attributes for specific args used by non-CLI classes
        if kwargs.get('full_tb') is None:
            self.full_tb = False

        
class PrismDAG(
    prism.mixins.compile.CompileMixin,
    prism.mixins.connect.ConnectMixin,
    prism.mixins.run.RunMixin
):
    """
    Class to access Prism infrastructure without the CLI
    """

    def __init__(self,
        project_dir: Path,
        profiles_dir: Optional[Path] = None,
        logging: bool = False
    ):
        self.project_dir = project_dir
        self.profiles_dir = project_dir if profiles_dir is None else profiles_dir
        self.logging = logging

        # Check if project is valid
        self._is_valid_project(self.project_dir)

        # Modules directory
        self.modules_dir = self.get_modules_dir(self.project_dir)

        # All modules in project
        self.all_modules = self.get_modules(self.modules_dir)

    
    def _is_valid_project(self, user_project_dir: Path):
        """
        Determine if `user_project_dir` is a valid project (i.e., that is has a `prism_project.py` file and a `modules` folder)

        args:
            user_project_dir: project path
        raises:
            exception if Prism project is not valid
        """
        os.chdir(user_project_dir)
        temp_project_dir = prism.cli.base.get_project_dir()
        if temp_project_dir!=user_project_dir:
            msg_list = [
                f'no project at `{str(user_project_dir)}',
                f'closest project found at `{str(temp_project_dir)}`'
            ]
            raise prism.exceptions.InvalidProjectException(message='\n'.join(msg_list))
        
        if not Path(user_project_dir / 'modules').is_dir():
            raise prism.exceptions.InvalidProjectException(message=f'`modules` directory not found in `{str(user_project_dir)}`')

    
    def connect(self, **kwargs):
        """
        Connect task to an adapter
        """
        connection_type = kwargs.get('type')
        if connection_type is None or not isinstance(connection_type, str):
            raise prism.exceptions.RuntimeException(message=f'connection type cannot be None')
        if connection_type not in prism.constants.VALID_ADAPTERS:
            msg_list = [
                f'invalid connection type `{connection_type}',
                f'must be one of {",".join(prism.constants.VALID_ADAPTERS)}'
            ]
            raise prism.exceptions.RuntimeException(message=f'\n'.join(msg_list))
        profiles_filepath = self.profiles_dir / 'profile.yml'
        self.create_connection(connection_type, profiles_filepath)


    def compile(self, **kwargs):
        """
        Compile the Prism project
        """
        user_arg_modules = kwargs.get('modules')
        if user_arg_modules is None:
            user_arg_modules = self.all_modules
        self.user_arg_modules = user_arg_modules
        compiled_dir = self.create_compiled_dir(self.project_dir)
        self.compiled_dir = compiled_dir
        return self.compile_dag(self.project_dir, self.compiled_dir, self.all_modules, self.user_arg_modules)

    
    def run(self, **kwargs):
        """
        Run the Prism project
        """
        env = kwargs.get('env')
        if env is None:
            env = "local"
        all_upstream = kwargs.get('all_upstream')
        profiles_path = self.profiles_dir / 'profile.yml'
        compiled_dag = self.compile(**kwargs)
        compiled_dag.add_full_path(self.modules_dir)
        prism_project = self.create_project(self.project_dir, profiles_path, env, "run")
        self.globals_namespace = prism.constants.GLOBALS_DICT.copy()
        threads = prism_project.thread_count
        dag_executor = prism_executor.DagExecutor(compiled_dag, all_upstream, threads)
        pipeline = self.create_pipeline(prism_project, dag_executor, self.globals_namespace)

        # Create args
        args = RunArgs(**kwargs)
        pipeline.exec(args)


# EOF