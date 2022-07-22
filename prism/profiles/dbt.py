"""
DBT adapter class definition. This definition uses source code from:
    
    https://github.com/fal-ai/fal

Modifications are made to ensure compatibility with the rest of prism's architecture

Table of Contents
- Imports
- Utils / helper classes
- Class definition
"""

#############
## Imports ##
#############

# Standard library imports
from dataclasses import dataclass
from datetime import datetime
import os
import pandas as pd
from pathlib import Path
from typing import List, Optional, Tuple
from uuid import uuid4
from typing import Any, Dict

# dbt imports
from dbt.config import RuntimeConfig
import dbt.flags as flags
from dbt.config.profile import read_user_config
import dbt.semver
import dbt.events.functions as events_functions
import dbt.tracking
from dbt.task.compile import CompileTask
from dbt.parser.manifest import ManifestLoader
from dbt.contracts.graph.manifest import Manifest, MaybeNonSource, Disabled
from dbt.contracts.graph.compiled import ManifestNode, CompileResultNode
from dbt.adapters.sql import SQLAdapter
import dbt.adapters.factory as adapters_factory
from dbt.contracts.sql import ResultTable, RemoteRunResult
from dbt.contracts.connection import AdapterResponse

# Prism-specific imports
from .adapter import Adapter
import prism.exceptions


############################
## Utils / helper classes ##
############################

@dataclass
class DbtRuntimeConfigArgs:
    project_dir: str
    profiles_dir: str
    threads: Optional[int]
    single_threaded: bool
    target: Optional[str]

@dataclass
class InitializeFlagsArgs:
    profiles_dir: str
    use_colors: Optional[bool]

@dataclass
class InitializeDbtCompileTaskArgs:
    selector_name: Optional[str]
    select: List[str]
    models: List[str]
    exclude: Tuple[str, str]
    state: Optional[Path]
    single_threaded: Optional[bool]


######################
## Class definition ##
######################

class Dbt(Adapter):
    """
    Class for connecting prism project to dbt project
    """

    def __init__(self, type, adapter_dict):
        self.type = type
        self.adapter_dict = adapter_dict
        dbt_project_dir, dbt_profiles_dir, dbt_profiles_target = self.parse_adapter_dict(self.adapter_dict, 'dbt')

        # Get project directory, profiles directory, and profiles target
        self.dbt_project_dir = dbt_project_dir
        self.dbt_profiles_dir = dbt_profiles_dir
        self.dbt_profiles_target = dbt_profiles_target

        # Get config
        self.initialize_dbt_flags(self.dbt_profiles_dir)
        config = self.get_dbt_runtime_config(
            self.dbt_project_dir,
            self.dbt_profiles_dir, 
            profile_target=self.dbt_profiles_target
        )
        
        # Initialize flags and compile task
        self.compile_task = self.initialize_compile_task(self.dbt_profiles_dir, config)
        dbt.tracking.initialize_tracking(self.dbt_profiles_dir)

        # Manifest
        self.manifest = self.get_dbt_manifest(config)

        # Get adapter
        self.adapter = self.get_dbt_adapter(config)


    def parse_adapter_dict(self,
        adapter_dict: Dict[str, Optional[str]],
        adapter_type: str,
        return_type: str = "list"
    ) -> Tuple[str, str, Optional[str]]:
        """
        Parse dbt adapter, represented as a dict

        args:
            adapter_dict: Snowflake adapter represented as a dictionary
            adapter_type: type of adapter (will always be dbt)
            return_type: output type; one of either "str" or "list"
        returns:
            dbt_project_directory, dbt_profiles_directory, dbt_profiles_target
        """
        if return_type not in ["str", "list"]:
            raise prism.exceptions.RuntimeException(message=f'invalid `{return_type}` in `{self.__class__.__name__}.parse_adapter_dict`, must be either "str" or "list"')

        # Get dbt_project_directory, dbt_profiles_directory, dbt_profiles_target
        dbt_project_dir = self.get_adapter_var(adapter_dict, "project_dir", adapter_type)

        # If the dbt_profiles_dir is not populated, assume the default
        try:
            dbt_profiles_dir = self.get_adapter_var(adapter_dict, "profiles_dir", adapter_type)
        except prism.exceptions.InvalidProfileException:
            dbt_profiles_dir = os.path.expanduser('~/.dbt/')

        try:
            dbt_profiles_target = self.get_adapter_var(adapter_dict, "profiles_target", adapter_type)
        except prism.exceptions.InvalidProfileException:
            dbt_profiles_target = None
        
        # For type hinting
        if not isinstance(dbt_project_dir, str):
            raise prism.exceptions.InvalidProfileException(message=f'invalid `dbt_project_dir` type `{str(type(dbt_project_dir))}`')
        if not isinstance(dbt_profiles_dir, str):
            raise prism.exceptions.InvalidProfileException(message=f'invalid `dbt_profiles_dir` type `{str(type(dbt_project_dir))}`')
        if not (isinstance(dbt_profiles_target, str) or dbt_profiles_target is None):
            raise prism.exceptions.InvalidProfileException(message=f'invalid `dbt_profiles_target` type `{str(type(dbt_project_dir))}`')
        return dbt_project_dir, dbt_profiles_dir, dbt_profiles_target
    

    def get_dbt_runtime_config(self,
        project_dir: str,
        profiles_dir: str,
        threads: Optional[int] = 1,
        profile_target: Optional[str] = None,
    ) -> RuntimeConfig:
        """
        Get dbt RuntimeConfig object
        
        args:
            project_dir: dbt project directory
            profiles_dir: dbt profiles dir
            threads: threads used for dbt run
            profile_target: target specified under profile for dbt project
        returns:
            RuntimeConfig object
        """

        args = DbtRuntimeConfigArgs(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            threads=threads,
            single_threaded=False,
            target=profile_target,
        )
        return RuntimeConfig.from_args(args)


    def initialize_dbt_flags(self,
        profiles_dir: str
    ):
        """
        Initializes the flags module from dbt. This module ensures that the dbt compile task does not throw an error.

        args:
            profiles_dir: dbt profiles directory
        """        
        args = InitializeFlagsArgs(profiles_dir, None)
        user_config = read_user_config(profiles_dir)
        try:
            flags.set_from_args(args, user_config)
        except TypeError:
            flags.set_from_args(args)

        # Set invocation id
        events_functions.set_invocation_id()

    
    def initialize_compile_task(self,
        profiles_dir: str,
        dbt_config: RuntimeConfig
    ):
        """
        Initialize the compile task and call _runtime_initialize(). This must be done after the flags module has been
        initialized.

        args:
            profiles_dir: directory of dbt profiles
        """
        # Initialize tracking
        dbt.tracking.initialize_tracking(profiles_dir)

        # All the arguments to the compile task can be None or empty
        selector_name = None
        select: List[str] = []
        exclude: Tuple[str, str] = tuple() # type: ignore
        state = None
        args = InitializeDbtCompileTaskArgs(selector_name, select, select, exclude, state, None)

        # Initialize compile task. No need to call _runtime_initialize, because we do not need to read the graph
        compile_task = CompileTask(args, dbt_config)
        return compile_task
    

    def get_dbt_manifest(self,
        dbt_config: RuntimeConfig
    ) -> Manifest:
        """
        Get dbt manifest; this must be called after the dbt compile task is initialized

        args:
            dbt_config: dbt RuntimeConfig
        """
        return ManifestLoader.get_full_manifest(dbt_config)

    
    def get_dbt_adapter(self,
        dbt_config: RuntimeConfig
    ):
        """
        Get adapter associated with dbt project

        args:
            dbt_config: dbt RuntimeConfig
        returns:
            dbt SQLAdapter
        """
        dbt.adapters.factory.reset_adapters()
        dbt.adapters.factory.register_adapter(dbt_config)
        adapter: SQLAdapter = adapters_factory.get_adapter(dbt_config)
        return adapter


    def get_parsed_model_node(self,
        target_model_name: str,
        target_model_package: Optional[str],
        project_dir: str,
        manifest: Manifest  
    ) -> CompileResultNode:
        """
        Get the node associated with the inputted target model

        args:
            target_model_name: name of model to retrieve from manifest
            target_package_name: package containing model
            project_dir: project directory
            manifest: dbt Manifest
        returns:
            node associated with inputted target model
        """
        # Use `resolve_ref` method from manifest class. For simple projects, it should be the case that `node_package`
        # is identical to `project_directory`. There may be more complex projects where this is not the case. We
        # implement the simple version for now.
        # TODO: test target model creation where node_package != project_dir
        target_model: MaybeNonSource = manifest.resolve_ref(
            target_model_name=target_model_name,
            target_model_package=target_model_package,
            current_project=project_dir,
            node_package=project_dir
        )

        # If model isn't found, then throw an error. 
        package_str = f"'{target_model_package}'." if target_model_package is not None else ""
        model_str = f"{package_str}'{target_model_name}'"
        if target_model is None:
            raise prism.exceptions.RuntimeException(message=f'could not find model dbt model `{model_str}`')

        # The model could be disabled
        if isinstance(target_model, Disabled):
            raise prism.exceptions.RuntimeException(message=f'dbt model `{model_str}` is disabled')

        return target_model


    def get_target_model_relation(self,
        target: CompileResultNode,
        adapter: SQLAdapter,
        manifest: Manifest
    ):
        """
        Get target model relation (i.e., the location of target model in syntax of project's SQL engine)

        args:
            target: target model node
            adapter: dbt adapter
            manifest: dbt Manifest
        returns:
            target model relation
        """
        
        # Create a unique adapter connection and avoid clashes
        name = "relation:" + str(hash(str(target))) + ":" + str(uuid4())
        relation = None
        with adapter.connection_named(name):
            adapter.set_relations_cache(manifest, True)
            relation = adapter.get_relation(target.database, target.schema, target.name)

        # If relation is still none, throw an error
        if relation is None:
            raise prism.exceptions.RuntimeException(message=f'cannot find relation for target `{target.unique_id}`')
        
        return relation

    
    def execute_sql(self,
        adapter: SQLAdapter,
        sql_query: str
    ) -> Tuple[AdapterResponse, RemoteRunResult]:
        """
        Use dbt adapter to execute SQL query
        
        args:
            adapter: dbt SQLAdapter
            sql_query: SQL query
        """
        # Create unique adapter connection to avoid clashes
        name = "SQL:" + str(hash(sql_query)) + ":" + str(uuid4())
        with adapter.connection_named(name):
            response, adapter_exec_result = adapter.execute(sql_query, auto_begin=True, fetch=True)

            # Create agate table
            table = ResultTable(
                column_names=list(adapter_exec_result.column_names),
                rows=[list(row) for row in adapter_exec_result],
            )
            
            # Create remote run result
            result = RemoteRunResult(
                raw_sql=sql_query,
                compiled_sql=sql_query,
                node=None,
                table=table,
                timing=[],
                logs=[],
                generated_at=datetime.utcnow(),
            )
            adapter.commit_if_has_connection()
        
        return response, result


    def remote_result_to_pd(self,
        result: RemoteRunResult
    ) -> pd.DataFrame:
        """
        Convert dbt RemoteRunResult to pandas DataFrame

        args:
            result: dbt RemoteRunResult
        returns:
            pd.DataFrame
        """
        return pd.DataFrame.from_records(
            result.table.rows, columns=result.table.column_names, coerce_float=True
        )

    
    def handle_ref(self,
        target_1: str,
        target_2: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Download a dbt model into a Pandas DataFrame:
        
        args:
            target_1, target_2: dbt model
        returns:
            pandas DataFrame
        """
        # Convert inputs into package and model name
        target_model_name = target_1
        target_package_name = None
        if target_2 is not None:
            target_package_name = target_1
            target_model_name = target_2
        
        # Get target model
        target_model = self.get_parsed_model_node(
            target_model_name,
            target_package_name,
            self.dbt_project_dir, 
            self.manifest
        )

        # Get relation
        target_model_relation = self.get_target_model_relation(
            target_model,
            self.adapter,
            self.manifest
        )

        # Download model
        query = f"SELECT * FROM {target_model_relation}"
        response, result = self.execute_sql(self.adapter, query)
        df = self.remote_result_to_pd(result)
        return df

    
    def execute(self, return_type: str = "list"):
        """
        The `execute` method generates a string containing code to be executed
        """

        execute_str = [
            "from prism.profiles.dbt import Dbt",
            "DbtProject = Dbt(type='dbt', adapter_dict={})".format(str(self.adapter_dict))
        ]
        if return_type=="list":
            return execute_str
        else:
            return '\n'.join(execute_str)
    

    def create_engine(self,
        adapter_dict: Dict[str, Any],
        adapter_type: str
    ):
        # Unlike SQL adapters, the class itself is all that is required to handle DBT refs. Do nothing.
        return
        

# EOF