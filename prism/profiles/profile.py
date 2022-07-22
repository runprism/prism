"""
Profile class

Table of Contents
- Imports
- Class definition
"""

#############
## Imports ##
#############

# Standard library imports
from typing import Any, Dict, Optional

# Prism-specific imports
from .meta import MetaAdapter
from .adapter import Adapter
import prism.exceptions
import prism.constants
import prism.logging
from prism.profiles import meta, adapter, pyspark, snowflake, bigquery, dbt


######################
## Class definition ##
######################


class Profile:    
    """
    Class associated with profile.yml. You can think of a Profile object as being a dictionary of adapters
    """

    def __init__(self,
        profile_yml: Dict[str, Any],
        profile_name: str,
        env: str
    ):
        self.profile_yml = profile_yml
        self.profile_name = profile_name
        self.env = env
        self.adapters_obj_dict: Dict[str, Adapter] = {}

        # Get named profile
        self.named_profile = self.load_named_profile(self.profile_yml, self.profile_name)
        
        # Check if all profiles exist
        self.bool_all_profiles_exist = self.all_profiles_exist(self.profile_name, self.profile_yml, self.named_profile, self.env, True)

        # Check top-level keys -- should only be 'adapters' and 'clusters'
        if self.bool_all_profiles_exist:
            self.check_profile_toplevel_keys(self.named_profile)
            self.check_nonempty_profile(self.named_profile)


    def check_profile_toplevel_keys(self,
        profile: Dict[str, Optional[Dict[str, Any]]],
    ) -> bool:
        """
        Check that `profile.yml` has at most two top-level keys, and that those top-level keys are either `adapter` or
        `clusters`

        args:
            profile: profile.yml represented as a dict
        """
        profile_keys = list(profile.keys())
        if len(profile_keys)>2:
            msg_list = [
                f"invalid keys in profile.yml",
                "should only be `adapters` and `clusters`"
            ]
            raise prism.exceptions.InvalidProfileException(message='\n'.join(msg_list))
        invalid_keys = list(set(profile_keys)-set(prism.constants.VALID_PROFILE_KEYS))
        if len(invalid_keys)>0:
            msg_list = [
                f"invalid keys in profile.yml `{invalid_keys}`",
                "should only be `adapters` and `clusters`"
            ]
            raise prism.exceptions.InvalidProfileException(message='\n'.join(msg_list))
        
        # If no exception has been raised, return true
        return True
    

    def check_nonempty_profile(self,
        profile: Dict[str, Optional[Dict[str, Any]]],
    ) -> bool:
        """
        Check that `profile.yml` is non-empty

        args:
            profile: profile.yml represented as a dict
        """
        if not isinstance(profile, dict):
            raise prism.exceptions.InvalidProfileException(message="invalid syntax in `profile.yml`")
        if profile=={} or profile is None:
            raise prism.exceptions.InvalidProfileException(message="named profile in profile.yml is empty")
        
        # If no exception has been raised, return true
        return True


    def load_named_profile(self,
        profile_yml: Dict[str, Optional[Dict[str, Any]]],
        profile_name: str
    ) -> Dict[str, Any]:
        """
        Load profile associated with {profile_name} from {profile_yml}

        args:
            profile_yml: profile.yml file represented as dict
            profile_name: name of profile to use for project
        returns:
            profile associated with inputted profile_name
        """
        # Find the dictionary associated with inputted name
        for k,v in profile_yml.items():
            if k==profile_name:
                if v is None:
                    return {}
                else:
                    return v
        
        # If code reaches this stage, then no dictionary has been returned yet. Return an empty dictionary, we will
        # throw the approriate error in downstream function.
        return {}

    
    def all_profiles_exist(self,
        profile_name: str,
        profile_yml: Dict[str, Optional[Dict[str, Any]]],
        named_profile: Dict[str, Any],
        env: str,
        fire_warnings=True
    ) -> bool:
        """
        Confirm that all profile requirements are met for pipeleine

        args:
            args: user arguments
            prism_project_py: prism_project.py represented as a str
            profiles_path: path to profile.yml
            fire_warnings: boolean indicating whether to fire warning console events; default is True
        returns:
            boolean indicating whether profile requirements are met
        """
        # Flags for missing profile name, empty profile YAML, and missing AWS CLI profile
        flag_missing_profile_yml = profile_yml=={}
        flag_missing_profile_name = profile_name=="" or profile_name is None
        flag_missing_named_profile = named_profile=={}

        if flag_missing_profile_yml:
            return False

        # Handle cases where the profile.yml is non-empty
        else:

            # If missing profile name, then the named profile will, by definition, be {}. Throw a warning and return 
            # False
            if flag_missing_profile_name:
                if fire_warnings:
                    e1 = prism.logging.ProfileNameDoesNotExistYamlExists()
                    prism.logging.fire_console_event(e1, [], 0)
                return False
            
            # If the profile name is not missing, check if the named profile is empty. If it is, then raise a warning
            # and return False.
            else:
                if flag_missing_named_profile:
                    if fire_warnings:
                        e2 = prism.logging.ProfileNameExistsYamlDoesNotExist()
                        prism.logging.fire_console_event(e2, [], 0)
                    return False
        
        # Nothing has been returned, return True
        return True


    def get_adapter_types(self):
        """
        Get types of adapters in named profile
        """
        if not self.bool_all_profiles_exist:
            return []
        else:
            try:
                return list(self.named_profile['adapters'].keys())
            
            # The profile.yml does not contain an adapters top-level key
            except KeyError:
                return []


    def generate_adapters(self):
        """
        Generate adapter objects
        """
        if not self.bool_all_profiles_exist:
            return {}
        else:
            # Iterate through adapters and check that they are supported
            try:
                adapters_dict = self.named_profile['adapters']
            except KeyError:
                adapters_dict = {}
                
            for conn in list(adapters_dict.keys()):

                # Check that adapters only include supported connections
                if conn not in prism.constants.VALID_ADAPTERS:
                    raise prism.exceptions.InvalidProfileException(message=f"invalid adapter `{conn}` in profile.yml")
                adapter = MetaAdapter.get_adapter(conn)(conn, adapters_dict[conn])
                self.adapters_obj_dict[conn] = adapter
    

    def get_adapters_obj_dict(self):
        """
        Get dictionary of adapter objects associated with profile
        """
        return self.adapters_obj_dict


# EOF