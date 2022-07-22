"""
Compile task class definition, called via `prism compile`

Table of Contents
- Imports
- Class definition
"""


#############
## Imports ##
#############

# Standard library imports
import os
import re
import argparse
from pathlib import Path
from typing import List, Tuple, Union

# Prism-specific imports
import prism.cli.base
import prism.exceptions
import prism.constants
import prism.logging
from prism.logging import Event, fire_console_event, fire_empty_line_event
from prism.event_managers import base as base_event_manager
from prism.infra import compiler


######################
## Class definition ##
######################

class CompileTask(prism.cli.base.BaseTask):
    """
    Class for compiling a prism project and computing the DAG
    """

    def __init__(self, args):
        self.args = args


    def is_valid_script_name(self,
        script: str
    ) -> bool:
        """
        Check if script name is valid. Script must start with [A-Za-z] character to be valid.

        args:
            script: script name
        returns:
            prism.exceptions.CompileException
        """
        if len(re.findall('\.py$', script))==0:
            pass
        else:
            if len(re.findall('(?i)^[a-z]', script))>0:
                return True
        raise prism.exceptions.CompileException(message=f'`{script}` is not valid name; script must start with [a-zA-Z] to be compiled correctly')
    

    def get_modules_dir(self,
        project_dir: Path
    ) -> Path:
        """
        Get directory containing modules
        
        args:
            project_dir: project directory
        returns:
            modules directory
        """
        modules_dir = project_dir / 'modules'
        if not modules_dir.is_dir():
            raise prism.exceptions.CompileException(message=f'`modules` directory not found in `{str(project_dir)}`')
        return modules_dir


    def get_modules(self,
        modules_dir: Path,
        prefix: Path = Path('')
    ) -> List[Path]:
        """
        Get all Python modules from `modules_dir`

        args:
            modules_dir: modules directory
            prefix: prefix to use for directories in modules directory
        returns:
            list of modules in directory (as pathlib Path objects)
        """
        modules = []
        for path in modules_dir.iterdir():

            # If object is a directory...
            if path.is_dir():

                # If parent directory is the modules folder, set the prefix to the name of the directory. For example,
                # if the modules folder has a directory called "extraction", then the modules within "extraction" should
                # be stored as "extraction/..."
                if str(prefix)=='.':
                    modules.extend(self.get_modules(path, prefix = Path(path.name)))
                
                # If parent directory is not the modules folder, set the prefix to the parent prefix + the name of the
                # directory. Using the above example, if the "extraction" folder has a directory called "step1", then
                # the modules within "step1" should be stored as "extraction/step1/..."
                else:
                    modules.extend(self.get_modules(path, prefix = Path(prefix) / path.name))
            
            # If object is a file
            elif path.is_file():

                # If file is a python file...
                if len(re.findall('\.py$', path.name))>0:

                    # If parent directory is the modules folder, then just add the python file name
                    if str(prefix)=='.':
                        modules+=[Path(path.name)]

                    # If parent directory is not the modules folder, then add prefix to python file name
                    else:
                        modules+=[Path(prefix) / Path(path.name)]
        return modules
    

    def user_arg_modules(self,
        args: argparse.Namespace,
        modules_dir: Path
    ) -> List[Path]:
        """
        Process user arg modules

        args:
            args: user arguments
            modules_dir: modules directory
        returns:
            list of processed user arg modules
        """
        try:
            raw_modules = args.modules
            if raw_modules is None:
                processed_modules = self.get_modules(modules_dir)
            else:
                processed_modules = []
                for m in raw_modules:
                    
                    # Check if * is used
                    m_split = m.split('/')
                    if m_split[-1]=='*':
                        parent = '/'.join([m_comp for m_comp in m_split[:-1]])
                        processed_modules.extend(self.get_modules(Path(modules_dir / parent), Path(parent)))
                    
                    # Check if path is a directory
                    elif Path(modules_dir / m).is_dir():
                        raise prism.exceptions.CompileException(f'invalid --modules argument `{m}`')
                        
                    # Check if path is a file
                    elif Path(modules_dir / m).is_file():
                        if len(re.findall('\.py$', Path(modules_dir / m).name))==0:
                            raise prism.exceptions.CompileException(f'invalid --modules argument `{m}`')
                        else:
                            processed_modules.extend([Path(m)])
                    
                    else:
                        raise prism.exceptions.CompileException(f'invalid object `{str(m)}` in `{str(modules_dir)}`')
                
        # If --modules argument not specified, then get all modules
        except AttributeError:
            processed_modules = self.get_modules(modules_dir)

        return processed_modules


    def get_compiled_dir(self,
        project_dir: Path
    ) -> Path:
        """
        Get directory containing compiled modules

        args:
            project_dir: project directory
        returns:
            compiled_dir: directory with compiled modules
        """
        compiled_dir = project_dir / '.compiled'
        return compiled_dir


    def create_compiled_dir(self,
        project_dir: Path
    ):
        """
        Create folder for compiled modules in project directory

        args:
            project_dir: project directory
            modules: list of modules to compile
        returns:
            None
        """

        # Create compiled directory
        compiled_modules_path = self.get_compiled_dir(project_dir)
        if not compiled_modules_path.is_dir():
            compiled_modules_path.mkdir(parents=True, exist_ok=True)
        return compiled_modules_path
    

    def compile_dag(self,
        project_dir: Path,
        compiled_dir: Path,
        all_modules: List[Path],
        user_arg_modules: List[Path]
    ) -> compiler.CompiledDag:
        """
        Wrapper for the `compile` method in the DagCompiler class

        args:
            project_dir: project directory
            all_modules: all modules in project
            user_arg_modules: modules passed in user argument
            compiler_globals: globals() dictionary
        returns:
            CompiledDag object
        """
        dag_compiler = compiler.DagCompiler(project_dir, compiled_dir, all_modules, user_arg_modules)
        compiled_dag = dag_compiler.compile()
        
        # Check that all modules in topological sort are in project
        for m in compiled_dag.topological_sort:
            if m not in all_modules:
                raise prism.exceptions.CompileException(message=f'module `{m}` not found in project')
        
        # Otherwise, return
        return compiled_dag


    def run(self) -> prism.cli.base.TaskRunReturnResult:
        """
        Run the compile task. This task is executed when the user runs the compile task from the CLI.
        
        args:
            None
        returns:
            None
        """

        # Keep track of events
        event_list: List[Event] = []

        # Fire header events
        event_list, project_dir = self.fire_header_events(event_list)
        if project_dir is None:
            return prism.cli.base.TaskRunReturnResult(event_list)
        os.chdir(project_dir)

        # Compiled dir
        compiled_dir = self.create_compiled_dir(project_dir)

        # Parse mod references from modules
        try:
            modules_dir = self.get_modules_dir(project_dir)
        except prism.exceptions.CompileException as err:
            e = prism.logging.PrismExceptionErrorEvent(err, 'accessing modules directory')
            event_list = fire_console_event(e, event_list, 0)
            event_list = fire_console_event(prism.logging.SeparatorEvent(), event_list, 0)
            return prism.cli.base.TaskRunReturnResult(event_list)
        user_arg_modules = self.user_arg_modules(self.args, modules_dir)
        all_modules = self.get_modules(modules_dir)
        event_list = fire_console_event(prism.logging.CompileStartEvent(len(all_modules), 'compile'), event_list)
        event_list = fire_empty_line_event(event_list)

        # Manager for compiling DAG
        compiler_manager = base_event_manager.BaseEventManager(
            args=self.args,
            idx=None,
            total=None,
            name='module DAG',
            func=self.compile_dag
        )
        compiled_dag, event_list = compiler_manager.manage_events_during_run(
            event_list=event_list,
            project_dir=project_dir,
            compiled_dir=compiled_dir,
            all_modules=all_modules,
            user_arg_modules=user_arg_modules
        )
        if compiled_dag==0:
            return prism.cli.base.TaskRunReturnResult(event_list)
        
        # Print output message if successfully executed
        event_list = fire_empty_line_event(event_list)
        event_list = fire_console_event(prism.logging.TaskSuccessfulEndEvent(), event_list, 0)
        event_list = fire_console_event(prism.logging.SeparatorEvent(), event_list, 0)

        # Return
        return prism.cli.base.TaskRunReturnResult(event_list)

    
    def run_for_subclass(self,
        args: argparse.Namespace,
        project_dir: Path,
        compiled_dir: Path,
        event_list: List[prism.logging.Event],
        fire_exec_events: bool = True
    ) -> Union[prism.cli.base.TaskRunReturnResult, Tuple[Union[None, compiler.CompiledDag], List[Event]]]:
        """
        Run the compile task. This task is executed when the user runs a subclass of the CompileTask (e.g., the RunTask)

        args: 
            args: user arguments
            globals_dict: globals() dict for exec
            fire_exec_events: boolean indicating whether to fire exec events associated with DAG creation
        returns: 
            dag: list of modules to run in sorted order
        """

        # Create compiled modules
        try:
            modules_dir = self.get_modules_dir(project_dir)
        except prism.exceptions.CompileException as err:
            e = prism.logging.PrismExceptionErrorEvent(err, 'accessing modules directory')
            event_list = fire_console_event(e, event_list, 0)
            event_list = fire_console_event(prism.logging.SeparatorEvent(), event_list, 0)
            return prism.cli.base.TaskRunReturnResult(event_list)
        user_arg_modules = self.user_arg_modules(self.args, modules_dir)
        all_modules = self.get_modules(modules_dir)

        # Manager for parsing node_dicts
        compiler_manager = base_event_manager.BaseEventManager(
            args=args,
            idx=None,
            total=None,
            name='module DAG',
            func=self.compile_dag
        )
        compiled_dag, event_list = compiler_manager.manage_events_during_run(
            event_list=event_list,
            fire_exec_events=fire_exec_events,
            project_dir=project_dir,
            compiled_dir=compiled_dir,
            all_modules=all_modules,
            user_arg_modules=user_arg_modules
        )
        if compiled_dag==0:
            return None, event_list

        # Update the paths using the compiled directory
        modules_dir = project_dir / 'modules'
        compiled_dag.add_full_path(modules_dir)
        return compiled_dag, event_list


# EOF