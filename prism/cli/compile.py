"""
Compile task class definition, called via `prism compile`

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Standard library imports
import os
import argparse
from pathlib import Path
from typing import List, Optional, Union

# Prism-specific imports
import prism.cli.base
import prism.mixins.compile
import prism.exceptions
import prism.constants
import prism.prism_logging
from prism.prism_logging import fire_console_event, fire_empty_line_event
from prism.event_managers.base import BaseEventManager, EventManagerOutput
from prism.infra.project import PrismProject


####################
# Class definition #
####################

class CompileTask(prism.cli.base.BaseTask, prism.mixins.compile.CompileMixin):
    """
    Class for compiling a prism project and computing the DAG
    """

    def run(self) -> prism.cli.base.TaskRunReturnResult:
        """
        Run the compile task. This task is executed when the user runs the compile task
        from the CLI.
        """

        # ------------------------------------------------------------------------------
        # Fire header events

        event_list, project_dir = self.fire_header_events()
        if project_dir is None:
            return prism.cli.base.TaskRunReturnResult(event_list)
        os.chdir(project_dir)

        # ------------------------------------------------------------------------------
        # Define directories and get modules to compile

        compiled_dir = self.create_compiled_dir(project_dir)

        # Modules directory
        try:
            modules_dir = self.get_modules_dir(project_dir)
        except prism.exceptions.CompileException as err:
            e = prism.prism_logging.PrismExceptionErrorEvent(
                err,
                'accessing modules directory'
            )
            event_list = fire_console_event(e, event_list, 0, 'error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list, True)

        # Get modules to compile
        user_arg_modules = self.user_arg_modules(self.args, modules_dir)
        all_modules = self.get_modules(modules_dir)
        event_list = fire_empty_line_event(event_list)

        # ------------------------------------------------------------------------------
        # Parse module references

        compiler_manager = BaseEventManager(
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
        if compiled_dag == 0:
            event_list = fire_console_event(
                event_to_fire,
                event_list,
                log_level='error'
            )
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list, True)

        # Print output message if successfully executed
        event_list = fire_empty_line_event(event_list)
        event_list = fire_console_event(
            prism.prism_logging.TaskSuccessfulEndEvent(),
            event_list,
            0,
            log_level='info'
        )
        event_list = self.fire_tail_event(event_list)

        # Return
        return prism.cli.base.TaskRunReturnResult(event_list)

    def run_for_subclass(self,
        args: argparse.Namespace,
        project_dir: Path,
        event_list: List[prism.prism_logging.Event],
        project: Optional[PrismProject] = None,
        fire_exec_events: bool = True
    ) -> Union[prism.cli.base.TaskRunReturnResult, EventManagerOutput]:
        """
        Run the compile task. This task is executed when the user runs a subclass of the
        CompileTask (e.g., the RunTask)

        args:
            args: user arguments
            project_dir: project directory
            compiled_dir: directory to store manifest.json
            event_list: list of events
            fire_exec_events: bool controlling whether to fire logging events
        returns:
            dag: list of modules to run in sorted order
        """

        # Modules directory
        try:
            modules_dir = self.get_modules_dir(project_dir)
        except prism.exceptions.CompileException as err:
            e = prism.prism_logging.PrismExceptionErrorEvent(
                err,
                'accessing modules directory'
            )
            event_list = fire_console_event(e, event_list, 0, log_level='error')
            event_list = self.fire_tail_event(event_list)
            return prism.cli.base.TaskRunReturnResult(event_list)

        # Modules to compile
        user_arg_modules = self.user_arg_modules(self.args, modules_dir)
        all_modules = self.get_modules(modules_dir)

        # All downstream
        all_downstream = args.all_downstream

        # Create compiled directory
        compiled_dir = self.create_compiled_dir(project_dir)

        # Parse module references
        compiler_manager = BaseEventManager(
            idx=None,
            total=None,
            name='module DAG',
            full_tb=args.full_tb,
            func=self.compile_dag
        )
        compiled_event_manager_output = compiler_manager.manage_events_during_run(
            event_list=event_list,
            fire_exec_events=fire_exec_events,
            project_dir=project_dir,
            compiled_dir=compiled_dir,
            all_modules=all_modules,
            user_arg_modules=user_arg_modules,
            user_arg_all_downstream=all_downstream,
            project=project
        )
        compiled_dag = compiled_event_manager_output.outputs
        if compiled_dag == 0:
            return compiled_event_manager_output

        return compiled_event_manager_output
