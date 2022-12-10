"""
Prism Module class

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Standard library imports
from typing import Any, Dict

# Prism-specific imports
from prism.infra import project as prism_project
from prism.infra import executor as prism_executor
from prism.infra import task_manager, hooks
import prism.constants
import prism.exceptions
import prism.logging
from prism.mixins import sys_handler
from prism.constants import INTERNAL_TASK_MANAGER_VARNAME, INTERNAL_HOOKS_VARNAME


####################
# Class definition #
####################

class PrismPipeline(sys_handler.SysHandlerMixin):
    """
    Class for managing prism project components and scope
    """

    def __init__(self,
        project: prism_project.PrismProject,
        executor: prism_executor.DagExecutor,
        run_context: Dict[Any, Any]
    ):
        self.project = project
        self.dag_executor = executor
        self.run_context = run_context

        # ------------------------------------------------------------------------------
        # sys.path configuration

        # Identify default modules loaded in sys.modules and paths loaded in sys.paths.
        # This will allow us to add/remove modules programatically without messing up
        # the base configuration.
        temp_context = {}
        exec('import sys', temp_context)
        self.base_sys_path = [p for p in temp_context['sys'].path]
        self.base_sys_modules = {
            k: v for k, v in temp_context['sys'].modules.items()
        }

        # Configure sys.path
        self.add_paths_to_sys_path(self.project.sys_path_config, self.run_context)

        # ------------------------------------------------------------------------------
        # Adapters, task manager, and hooks

        adapter_types = []
        adapter_objs = []
        for _, adapter in self.project.adapters_object_dict.items():
            adapter_types.append(adapter.adapter_dict['type'])
            adapter_objs.append(adapter)

        # The pipeline is being run using spark-submit, then the adapters must contain a
        # pyspark adapter
        if self.project.which == 'spark-submit':
            if 'pyspark' not in adapter_types:
                raise prism.exceptions.RuntimeException(
                    message='`spark-submit` command requires a `pyspark` adapter'
                )

        # If the profile.yml contains a pyspark adapter, then the user should use the
        # spark-submit command
        if 'pyspark' in adapter_types:
            if self.project.which == 'run':
                raise prism.exceptions.RuntimeException(
                    message='`pyspark` adapter found in profile.yml, use `spark-submit` command'  # noqa; E501
                )

        # Create task_manager and hooks objects
        task_manager_obj = task_manager.PrismTaskManager(upstream={})
        hooks_obj = hooks.PrismHooks(self.project)

        # If PySpark adapter is specified in the profile, then explicitly add
        # SparkSession to hooks
        if 'pyspark' in adapter_types:
            for atype, aobj in zip(adapter_types, adapter_objs):
                if atype == "pyspark":
                    pyspark_alias = aobj.get_alias()
                    pyspark_spark_session = aobj.engine
                    setattr(hooks_obj, pyspark_alias, pyspark_spark_session)

        self.run_context[INTERNAL_TASK_MANAGER_VARNAME] = task_manager_obj
        self.run_context[INTERNAL_HOOKS_VARNAME] = hooks_obj

        # ------------------------------------------------------------------------------
        # Set the globals for the executor

        self.dag_executor.set_executor_globals(self.run_context)

    def exec(self, full_tb: bool):
        """
        Execute pipeline
        """
        executor_output = self.dag_executor.exec(full_tb)

        # Close SQL adapter connections
        if "snowflake" in list(self.project.adapters_object_dict.keys()):
            self.project.adapters_object_dict["snowflake"].engine.close()
        if "redshift" in list(self.project.adapters_object_dict.keys()):
            self.project.adapters_object_dict["redshift"].engine.close()
        if "bigquery" in list(self.project.adapters_object_dict.keys()):
            self.project.adapters_object_dict["bigquery"].engine.close()

        # Remove project dir and all associated modules from sys path
        self.run_context = self.remove_paths_from_sys_path(
            self.base_sys_path, self.project.sys_path_config, self.run_context
        )
        self.run_context = self.remove_project_modules(
            self.base_sys_modules, self.project.sys_path_config, self.run_context
        )
        return executor_output
