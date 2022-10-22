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
import argparse
from dataclasses import dataclass
from typing import Any, Dict
import copy

# Prism-specific imports
from prism.infra import project as prism_project
from prism.infra import executor as prism_executor
from prism.infra import task_manager, hooks
import prism.constants
import prism.exceptions
import prism.logging
from prism.mixins import sys_handler
from prism.constants import INTERNAL_TASK_MANAGER_VARNAME, INTERNAL_HOOKS_VARNAME


######################
## Class definition ##
######################


class PrismPipeline(sys_handler.SysHandlerMixin):
    """
    Class for managing prism project components and scope
    """

    def __init__(self,
        project: prism_project.PrismProject,
        executor: prism_executor.DagExecutor,
        pipeline_globals: Dict[Any, Any]
    ):
        self.project = project
        self.dag_executor = executor
        self.pipeline_globals = pipeline_globals

        # ------------------------------------------------------------------------------------------
        # sys.path configuration

        # Before executing anything, keep track of modules loaded in sys.modules and paths loaded
        # in sys.paths. This will allow us to add/remove modules programatically without messing
        # up the base configuration
        if 'sys' not in self.pipeline_globals.keys():
            exec('import sys', self.pipeline_globals)
        self.base_sys_path = [p for p in self.pipeline_globals['sys'].path]
        self.base_sys_modules = {k:v for k,v in self.pipeline_globals['sys'].modules.items()}

        # Execute project
        self.project.exec(self.pipeline_globals)

        # Compiled sys.path config
        try:
            self.sys_path_config = self.pipeline_globals['SYS_PATH_CONF']

            # If project directory not in sys_path_config, throw a warning
            if str(self.project.project_dir) not in [str(p) for p in self.sys_path_config]:
                prism.logging.fire_console_event(prism.logging.ProjectDirNotInSysPath(), [], log_level='warn')

        # Fire a warning, even if the user specified `quietly`
        except KeyError:
            prism.logging.fire_console_event(prism.logging.SysPathConfigWarningEvent(), [], log_level='warn')
            self.sys_path_config = [self.project.project_dir]
        
        # Configure sys.path. Before adding 
        self.add_paths_to_sys_path(self.sys_path_config, self.pipeline_globals)


        # ------------------------------------------------------------------------------------------
        # Adapters, task manager, and hooks

        adapter_types = []
        adapter_objs = []
        for _, adapter in self.project.adapters_object_dict.items():
            adapter_types.append(adapter.adapter_dict['type'])
            adapter_objs.append(adapter)

        # The pipeline is being run using spark-submit, then the adapters must contain a pyspark adapter
        if self.project.which=='spark-submit':
            if 'pyspark' not in adapter_types:
                raise prism.exceptions.RuntimeException(message='`spark-submit` command requires a `pyspark` adapter')
            
        # If the profile.yml contains a pyspark adapter, then the user should use the spark-submit command
        if 'pyspark' in adapter_types:
            if self.project.which=='run':
                raise prism.exceptions.RuntimeException(message='`pyspark` adapter found in profile.yml, use `spark-submit` command')

        # Create task_manager and hooks objects
        task_manager_obj = task_manager.PrismTaskManager(upstream={})
        hooks_obj = hooks.PrismHooks(self.project)
        
        # If PySpark adapter is specified in the profile, then explicitly add SparkSession to hooks
        if 'pyspark' in adapter_types:
            for atype, aobj in zip(adapter_types, adapter_objs):
                if atype=="pyspark":
                    pyspark_alias = aobj.get_alias()
                    pyspark_spark_session = aobj.engine
                    setattr(hooks_obj, pyspark_alias, pyspark_spark_session)

        self.pipeline_globals[INTERNAL_TASK_MANAGER_VARNAME] = task_manager_obj
        self.pipeline_globals[INTERNAL_HOOKS_VARNAME] = hooks_obj


        # ------------------------------------------------------------------------------------------
        # Set the globals for the executor

        self.dag_executor.set_executor_globals(self.pipeline_globals)


    def exec(self, full_tb: bool):
        executor_output = self.dag_executor.exec(full_tb)
        
        # Close SQL adapter connections
        if "snowflake" in list(self.project.adapters_object_dict.keys()):
            self.project.adapters_object_dict["snowflake"].engine.close()
        if "redshift" in list(self.project.adapters_object_dict.keys()):
            self.project.adapters_object_dict["redshift"].engine.close()
        if "bigquery" in list(self.project.adapters_object_dict.keys()):
            self.project.adapters_object_dict["bigquery"].engine.close()
        
        # Remove project dir and all associated modules from sys path
        self.pipeline_globals = self.remove_paths_from_sys_path(self.base_sys_path, self.sys_path_config, self.pipeline_globals)
        self.pipeline_globals = self.remove_project_modules(self.base_sys_modules, self.sys_path_config, self.pipeline_globals)
        
        return executor_output



# EOF