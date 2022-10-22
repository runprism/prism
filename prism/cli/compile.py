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
import prism.mixins.compile
import prism.exceptions
import prism.constants
import prism.logging
from prism.logging import Event, fire_console_event, fire_empty_line_event
from prism.event_managers import base as base_event_manager
from prism.infra import compiler


######################
## Class definition ##
######################

class CompileTask(prism.cli.base.BaseTask, prism.mixins.compile.CompileMixin):
    """
    Class for compiling a prism project and computing the DAG
    """


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
            event_list = fire_console_event(e, event_list, 0, 'error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)
        user_arg_modules = self.user_arg_modules(self.args, modules_dir)
        all_modules = self.get_modules(modules_dir)
        event_list = fire_console_event(prism.logging.CompileStartEvent(len(all_modules), 'compile'), event_list, log_level='info')
        event_list = fire_empty_line_event(event_list)

        # Manager for compiling DAG
        compiler_manager = base_event_manager.BaseEventManager(
            idx=None,
            total=None,
            name='module DAG',
            full_tb=self.args.full_tb,
            func=self.compile_dag
        )
        compiled_event_manager_output = compiler_manager.manage_events_during_run(
            event_list=event_list,
            project_dir=project_dir,
            compiled_dir=compiled_dir,
            all_modules=all_modules,
            user_arg_modules=user_arg_modules
        )
        compiled_dag = compiled_event_manager_output.outputs
        event_to_fire = compiled_event_manager_output.event_to_fire
        event_list = compiled_event_manager_output.event_list
        if compiled_dag==0:
            event_list = fire_empty_line_event(event_list)
            event_list = fire_console_event(event_to_fire, event_list, log_level='error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)
        
        # Print output message if successfully executed
        event_list = fire_empty_line_event(event_list)
        event_list = fire_console_event(prism.logging.TaskSuccessfulEndEvent(), event_list, 0, log_level='info')
        event_list = self.fire_tail_event(event_list)

        # Return
        return prism.cli.base.TaskRunReturnResult(event_list)

    
    def run_for_subclass(self,
        args: argparse.Namespace,
        project_dir: Path,
        compiled_dir: Path,
        event_list: List[prism.logging.Event],
        fire_exec_events: bool = True
    ) -> Union[prism.cli.base.TaskRunReturnResult, base_event_manager.EventManagerOutput]:
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
            event_list = fire_console_event(e, event_list, 0, log_level='error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)
        user_arg_modules = self.user_arg_modules(self.args, modules_dir)
        all_modules = self.get_modules(modules_dir)

        # Manager for parsing node_dicts
        compiler_manager = base_event_manager.BaseEventManager(
            idx=None,
            total=None,
            name='module DAG',
            full_tb=args.full_tb,
            func=self.compile_dag
        )
        
        compiled_event_manager_output: base_event_manager.EventManagerOutput = compiler_manager.manage_events_during_run(
            event_list=event_list,
            fire_exec_events=fire_exec_events,
            project_dir=project_dir,
            compiled_dir=compiled_dir,
            all_modules=all_modules,
            user_arg_modules=user_arg_modules
        )
        compiled_dag = compiled_event_manager_output.outputs
        if compiled_dag==0:
            return compiled_event_manager_output

        return compiled_event_manager_output


# EOF