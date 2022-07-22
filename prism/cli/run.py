"""
Run task class definition, called via `prism run`

Table of Contents
- Imports
- Class definition
"""


#############
## Imports ##
#############

# Standard library imports
import os
import argparse
from typing import Any, Dict, List
from pathlib import Path

# Prism-specific imports
import prism.cli.base
import prism.cli.compile
import prism.exceptions
import prism.constants
import prism.logging
from prism.logging import Event, fire_console_event, fire_empty_line_event
from prism.event_managers import base as base_event_manager
from prism.infra import project as prism_project
from prism.infra import pipeline as prism_pipeline
from prism.infra import executor as prism_executor


class RunTask(prism.cli.compile.CompileTask):
    """
    Class for defining the "run" task
    """

    def __init__(self, args):
        # Initialize compile task
        super().__init__(args)
        self.args = args
 

    def parse_functions(self):
        return None
    
    
    def get_profile_path(self,
        args: argparse.Namespace,
        project_dir: Path
    ) -> Path:
        """
        Get profile.yml path from args

        args:
            args: user arguments
            project_dir: project directory
        returns:
            profiles_path: path to profile.yml
        """
        profile_dir = Path(args.profiles_dir) if args.profiles_dir is not None else project_dir
        profiles_path = profile_dir / 'profile.yml'
        return profiles_path

    
    def create_project(self,
        project_dir: Path,
        profiles_path: Path,
        env: str,
        which: str
    ) -> prism_project.PrismProject:
        """
        Wrapper for creation of PrismPipeline object. Needed in order to be compatible with event manager.

        args:
            code: str or code object to run
            globals_dict: globals dictionary
        returns:
            PrismPipeline object
        """
        project = prism_project.PrismProject(project_dir, profiles_path, env, which)
        project.setup()
        return project

    
    def create_pipeline(self,
        project: prism_project.PrismProject,
        dag_executor: prism_executor.DagExecutor,
        pipeline_globals: Dict[Any, Any]
    ) -> prism_pipeline.PrismPipeline:
        """
        Wrapper for creation of PrismPipeline object. Needed in order to be compatible with event manager.

        args:
            code: str or code object to run
            globals_dict: globals dictionary
        returns:
            PrismPipeline object
        """
        pipeline = prism_pipeline.PrismPipeline(project, dag_executor, pipeline_globals)
        return pipeline


    def run(self) -> prism.cli.base.TaskRunReturnResult:
        """
        Execute run task. Function is organized as follows:

        - Fire header events
        - Get location of config files, and get modules to compile
        - Create compiled DAG
        - Create prism project
        - Create pipeline
        - Execute pipeline
        - Fire footer events
        """

        # Keep track of events
        event_list: List[Event] = []

        # ---------------------------------------------------------------------------------------------------------
        # Fire header events
        
        event_list, project_dir = self.fire_header_events(event_list)
        if project_dir is None:
            return prism.cli.base.TaskRunReturnResult(event_list)
        os.chdir(project_dir)

        # Compiled dir
        compiled_dir = self.create_compiled_dir(project_dir)

        # ----------------------------------------------------------------------------------------------------------
        # Get location of config files, and get modules to compile
        
        profiles_path = self.get_profile_path(self.args, project_dir)

        # Get modules to compile
        try:
            modules_dir = self.get_modules_dir(project_dir)
        except prism.exceptions.CompileException as err:
            e = prism.logging.PrismExceptionErrorEvent(err, 'accessing modules directory')
            event_list = fire_console_event(e, event_list, 0)
            event_list = fire_console_event(prism.logging.SeparatorEvent(), event_list, 0)
            return prism.cli.base.TaskRunReturnResult(event_list)
        user_arg_modules = self.user_arg_modules(self.args, modules_dir)
        event_list = fire_console_event(prism.logging.CompileStartEvent(len(user_arg_modules), 'execute'), event_list)
        event_list = fire_empty_line_event(event_list)

        # ----------------------------------------------------------------------------------------------------------
        # Create compiled DAG
        
        result = super().run_for_subclass(self.args, project_dir, compiled_dir, event_list, True)
        if isinstance(result, prism.cli.base.TaskRunReturnResult):
            return result
        
        compiled_dag = result[0]
        event_list = result[1]
        
        # If no modules in DAG, return
        if compiled_dag is None:
            return prism.cli.base.TaskRunReturnResult(event_list)


        # ----------------------------------------------------------------------------------------------------------
        # Create prism project

        project_event_manager = base_event_manager.BaseEventManager(
            args=self.args,
            idx=None,
            total=None,
            name='parsing config files',
            func=self.create_project
        )
        prism_project, event_list = project_event_manager.manage_events_during_run(
            event_list=event_list,
            project_dir=project_dir,
            profiles_path=profiles_path,
            env="local",
            which=self.args.which
        )
        if prism_project==0:
            return prism.cli.base.TaskRunReturnResult(event_list)
        
        
        # ----------------------------------------------------------------------------------------------------------
        # Create pipeline

        # First, create DAG executor
        dag_executor = prism_executor.DagExecutor(compiled_dag, self.args.all_upstream)

        # Create pipeline
        self.globals_namespace = prism.constants.GLOBALS_DICT.copy()
        
        # Manager for creating pipeline
        pipeline_manager = base_event_manager.BaseEventManager(
            args=self.args,
            idx=None,
            total=None,
            name='creating pipeline, DAG executor',
            func=self.create_pipeline
        )
        pipeline, event_list = pipeline_manager.manage_events_during_run(
            event_list=event_list,
            project=prism_project,
            dag_executor=dag_executor,
            pipeline_globals=self.globals_namespace
        )
        if pipeline==0:
            return prism.cli.base.TaskRunReturnResult(event_list)


        # ----------------------------------------------------------------------------------------------------------
        # Execute pipeline

        event_list = fire_empty_line_event(event_list)

        # Manager for executing pipeline
        pipeline_exec_manager = base_event_manager.BaseEventManager(
            args=self.args,
            idx=None,
            total=None,
            name='executing pipeline',
            func=pipeline.exec
        )
        success, event_list = pipeline_exec_manager.manage_events_during_run(
            fire_exec_events=False,
            event_list=event_list,
            args=self.args
        )
        success_output = success[0]
        success_events = success[1]
        if success_output==0:
            event_list.extend(success_events)
            return prism.cli.base.TaskRunReturnResult(event_list)
        else:
            event_list.extend(success_events)
        
        
        # ----------------------------------------------------------------------------------------------------------
        # Fire footer events

        event_list = fire_empty_line_event(event_list)
        event_list = fire_console_event(prism.logging.TaskSuccessfulEndEvent(), event_list, 0)
        event_list = fire_console_event(prism.logging.SeparatorEvent(), event_list, 0)

        # Return
        return prism.cli.base.TaskRunReturnResult(event_list)


# EOF