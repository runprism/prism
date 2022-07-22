"""
Connect task class definition, called via `prism connect`

Table of Contents
- Imports
- Class definition
"""


#############
## Imports ##
#############

# Standard library imports
import os
import yaml
import shutil
from pathlib import Path
from typing import Any, Dict, List

# Prism-specific imports
import prism.cli.base
import prism.cli.compile
import prism.exceptions
import prism.constants
import prism.logging
from prism.event_managers.base import BaseEventManager
from prism.logging import Event, fire_console_event, fire_empty_line_event
from prism.templates.profile import PROFILES_TEMPLATE_DIR as profiles_template_dir


######################
## Class definition ##
######################

class ConnectTask(prism.cli.base.BaseTask):
    """
    Class for connecting a prism project to an external data warehouse
    (e.g., snowflake), a big-data processing system (e.g., PySpark), 
    and/or a dbt project
    """

    def create_directory(self,
        path: str
    ):
        """
        Create a directory at `path` if it doesn't exist.

        args:
            path: path to create a directory
        returns:
            None
        """
        if not Path(path).is_dir():
            p = Path(path)
            p.mkdir(parents=True, exist_ok=True)

    
    def update_yml(self,
        base_yml: Dict[str, Any],
        new_profile: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Dump `new_profile` into `base_yml`. Note that `new_profile` should only have one key

        args:
            base_yml: YML file to update
            new_profile: new dictionary to dump into YML
        returns:
            base_yml dictionary with new_profile appended on
        """
        # If the profile is empty, then throw an error
        if base_yml=={}:
            raise prism.exceptions.InvalidProfileException(message="`profile.yml` is empty")

        # `base_yml` should have a single key representing the profile name
        keys = list(base_yml.keys())
        values = list(base_yml.values())
        if len(keys)!=1 or len(values)!=1:
            raise prism.exceptions.InvalidProfileException(message="invalid `profile.yml`")
        
        # The values for `base_yml` should be a dictionary
        values_dict = list(base_yml.values())[0]
        if not isinstance(values_dict, dict):
            raise prism.exceptions.InvalidProfileException(message="invalid `profile.yml`; values should be nested dictionaries")

        # The new dictionary should have a single key representing the profile to add
        new_profile_keys = list(new_profile.keys())
        new_profile_values = list(new_profile.values())
        if len(new_profile_keys)!=1 or len(new_profile_values)!=1:
            raise prism.exceptions.InvalidProfileException(message="new profile not properly formatted")

        # The values of the new dictionary should be a dictionary
        new_profile_values_dict = list(new_profile.values())[0]
        if not isinstance(new_profile_values_dict, dict):
            raise prism.exceptions.InvalidProfileException(message="new profile not properly formatted")

        # Check that the profile.yml is properly structured
        profile_body = list(base_yml.values())[0]
        profile_keys = list(profile_body.keys())
        invalid_keys = list(set(profile_keys)-set(prism.constants.VALID_PROFILE_KEYS))
        if len(invalid_keys)>0:
            msg_list = [
                f"invalid keys in profile.yml `{invalid_keys}`",
                "should only be `adapters` and `clusters`"
            ]
            raise prism.exceptions.InvalidProfileException(message='\n'.join(msg_list))
        
        # Profile type must be a valid adapter or cluster
        profile_type = list(new_profile.keys())[0]
        if profile_type not in prism.constants.VALID_CONNECTIONS:
            raise prism.exceptions.InvalidProfileException(message=f"invalid type `{profile_type}`")
        
        # Check if profile already exists in adapters or clusters
        if profile_type in prism.constants.VALID_ADAPTERS:
            try:
                for adapter_type, adapter_body in profile_body['adapters'].items():
                    if profile_type==adapter_type:
                        raise prism.exceptions.InvalidProfileException(message=f"profile of type `{profile_type}` already found in profile.yml")
            
            # THe 'adapters' section isn't defined as of yet
            except KeyError:
                profile_body['adapters'] = {}

        # If new_profile is an adapter, add the profile to the `adapters` section of `profile.yml`. Otherwise, add it to
        # the `clusters` section.
        new_profile = list(new_profile.values())[0]
        if profile_type in prism.constants.VALID_ADAPTERS:
            profile_body['adapters'][profile_type] = new_profile

        # Return the revised base_yml
        return base_yml


    def create_profile_from_template(self,
        type: str,
        profiles_filepath: Path
    ):
        """
        Create a profiles.yml file using the template.
        
        args:
            type: connection type; one of "snowflake", "pyspark", or "dbt"
            profiles_filepath: location to keep the the profile.yml file
        returns:
            None
        """

        # We only ever call this function after confirming that the profile.yml file does not exist and that 
        # the type is valid.
        profiles_template_path = Path(profiles_template_dir) / type / f'profile.yml'
        shutil.copyfile(profiles_template_path, profiles_filepath)
    

    def create_connection(self,
        profile_type: str,
        profiles_filepath: Path
    ): 
        """
        Create a connection for the inputted `profile_type`

        args:
            profile_type: profile type
            profiles_filepath: path to profile.yml
        returns:
            profile.yml with added profile of type `profile_type`
        
        """
        # If the profile doesn't exist, then create it
        if not profiles_filepath.is_file():
            self.create_profile_from_template(profile_type, profiles_filepath)
            return

        # If the profile does exist, then update the profile based on the profile type
        with open(profiles_filepath) as f:
            base_yml = yaml.safe_load(f)
        f.close()

        template_path = Path(profiles_template_dir) / profile_type / f'profile.yml'
        with open(template_path, 'r') as f:
            template_yml = yaml.safe_load(f)
        f.close()
        new_connection = template_yml['profile_name']

        if profile_type in prism.constants.VALID_ADAPTERS:
            new_connection = new_connection['adapters']
        else:
            msg_list = [
                f"new profile_type is invalid",
                f"must be one of `{prism.constants.VALID_CONNECTIONS}`"
            ]
            raise prism.exceptions.InvalidProfileException(message='\n'.join(msg_list))

        # Update the template_yml
        base_yml_updated = self.update_yml(base_yml, new_connection)

        # Save the new profile name
        with open(profiles_filepath, 'w') as f:
            yaml.dump(base_yml_updated, f)
        f.close()


    def run(self) -> prism.cli.base.TaskRunReturnResult:
        """
        Execute connect task
        """
        # Keep track of events
        event_list: List[Event] = []

        # Fire header events
        event_list, project_dir = self.fire_header_events(event_list)
        if project_dir is None:
            return prism.cli.base.TaskRunReturnResult(event_list)

        # Change working directory to project directory
        os.chdir(project_dir)

        event_list = fire_empty_line_event(event_list)
        
        # Define profile type
        profile_type = self.args.type
        if profile_type is None:
            e = prism.logging.InvalidProfileType(None)
            event_list = fire_console_event(e, event_list, 0)
            event_list = fire_console_event(prism.logging.SeparatorEvent(), event_list, 0)
            return prism.cli.base.TaskRunReturnResult(event_list)
        elif profile_type not in prism.constants.VALID_CONNECTIONS:
            e = prism.logging.InvalidProfileType(profile_type)
            event_list = fire_console_event(e, event_list, 0)
            event_list = fire_console_event(prism.logging.SeparatorEvent(), event_list, 0)
            return prism.cli.base.TaskRunReturnResult(event_list)
        
        # Fire events
        event_list = fire_console_event(prism.logging.SettingUpProfileEvent(), event_list)

        # Create the profiles directory and profiles.yml file
        profiles_dir = project_dir if self.args.profiles_dir is None else self.args.profiles_dir
        profiles_filepath = Path(profiles_dir) / 'profile.yml'

        # Create a event manager for the connection setup
        profile_connection_event_manager = BaseEventManager(
            args=self.args,
            idx=None,
            total=None,
            name='connection setup',
            func=self.create_connection
        )
        success, event_list = profile_connection_event_manager.manage_events_during_run(
            event_list=event_list,
            fire_exec_events=False,
            profile_type=profile_type,
            profiles_filepath=profiles_filepath
        )
        if success==0:
            return prism.cli.base.TaskRunReturnResult(event_list)
        
        # Fire footer events
        event_list = fire_empty_line_event(event_list)
        event_list = fire_console_event(prism.logging.TaskSuccessfulEndEvent(), event_list, 0)
        event_list = fire_console_event(prism.logging.SeparatorEvent(), event_list, 0)

        return prism.cli.base.TaskRunReturnResult(event_list)


# EOF