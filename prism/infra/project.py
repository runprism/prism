"""
Prism Project class

Table of Contents
- Imports
- Class definition
"""

#############
## Imports ##
#############

# Standard library imports
import ast
import jinja2
import os
from pathlib import Path
from typing import Any, Dict, Optional

# Prism-specific imports
import prism.exceptions
import prism.logging
from prism.parsers import yml_parser
from prism.profiles import profile


######################
## Class definition ##
######################

class PrismProject:
    """
    Class to represent configuration files (prism_project.py and profile.yml)
    """

    def __init__(self,
        project_dir: Path,
        profiles_path: Path,
        env: str,
        which: str,
        filename='prism_project.py'
    ):

        # Set project directory and load prism_project.py
        self.project_dir = project_dir
        self.profiles_path = profiles_path
        self.env = env
        self.which = which
        self.filename = filename
        self.prism_project_py_str = self.load_prism_project_py(self.project_dir, self.filename)

    
    def setup(self):
        """
        Set up prism project. This should always be directly after object instantiation (except for in testing).
        """

        # Get profile name
        self.profile_name = self.get_profile_name(self.prism_project_py_str)

        # Set profiles path, load profile.yml, get named profile
        self.profile_yml = self.load_profile_yml(self.profiles_path)

        # Create Profile object
        self.profile = profile.Profile(self.profile_yml, self.profile_name, self.env)

        # Get adapters dict from profile
        self.profile.generate_adapters()
        self.adapters_object_dict = self.profile.get_adapters_obj_dict()
    

    def load_prism_project_py(self,
        project_dir: Path,
        filename
    ) -> str:
        """
        Load the prism_project.py file as a string

        args:
            project_dir: project directory
            filename: name of prism_project.py file (to facilitate testing); default is "prism_project.py"
            type: output type of python file; one of either "str" or "list"
        returns:
            prism_project_py: string representation of prism_project.py
        """
        os.chdir(project_dir)
        prism_project_py_path = project_dir / filename

        # Return file as string
        with open(prism_project_py_path, 'r') as f:
            prism_project_py = f.read()
        f.close()
        return prism_project_py

    
    def num_var_assignments_in_file(self,
        python_file: str,
        var: str
    ) -> int:
        """
        Get the number of times `var` is assigned a value in `python_file`

        args:
            python_file: Python file represented as a str
            var: variable of interest
        returns:
            number of times `var` is assigned a value in `python_file`
        """
        ast_module = ast.parse(python_file)
        
        # Only focus on the body; don't focus on contents of any functions / classes
        assigns = []
        for elem in ast_module.body:
            if isinstance(elem, ast.Assign):
                assigns.append(elem)
        
        # Check targets
        num_assignments = 0
        for obj in assigns:
            for target in obj.targets:
                
                # If target is not an ast.Name (e.g., a, b = "Hello", "world"), then
                # iterate through sub-notes of the target until we see an ast.Name
                if not isinstance(target, ast.Name):
                    for sub_elem in ast.walk(target):
                        if isinstance(sub_elem, ast.Name):
                            if sub_elem.id==var:
                                num_assignments+=1
                        else:
                            continue
                
                # Otherwise, check name of assignment var
                else:
                    if target.id==var:
                        num_assignments+=1
        return num_assignments

    
    def safe_eval_var_from_file(self,
        python_file: str,
        var: str,
    ) -> Optional[Any]:
        """
        Get `var` from `python_file`

        args:
            python_file: Python file represented as a str
            var: variable of interest
        returns:
            value of first `var` assignment in `python_file`
        """
    
        # If `python_file` is None, throw an error
        if python_file is None:
            raise prism.exceptions.InvalidProjectPyException(message="`prism_project.py` is undefined")

        # If multiple assignments, throw an error
        num_assigments = self.num_var_assignments_in_file(python_file, var)
        if num_assigments>1:
            raise prism.exceptions.InvalidProjectPyException(message=f"multiple assignments for `{var}` in `prism_project.py`")

        # Parse the Python file. We will wrap this function in an event manager, so no need to handle exceptions at the
        # moment.
        ast_data = ast.parse(python_file)
        for elem in ast_data.body:
            if isinstance(elem, ast.Assign):
                if len(elem.targets) == 1:
                    if getattr(elem.targets[0], "id", "") == var:
                        return ast.literal_eval(elem.value)
        return None


    def get_profile_name(self, 
        prism_project_py: str    
    ) -> str:
        """
        Get profile name from prism_project.py file

        args:
            prism_project_py: prism_project.py file represented as string
        returns:
            profile_name
        """
        profile_name = self.safe_eval_var_from_file(prism_project_py, 'profile')
        if profile_name is None:
            return ""
        if not isinstance(profile_name, str):
            return ""
        return profile_name
    

    def load_profile_yml(self,
        profiles_path: Path
    ) -> Dict[Any, Any]:
        """
        Load profile.yml file

        args:
            profiles_path: path to profile.yml
        returns:
            profile_yml: profile.yml file represented as a dict
        """
        try:
            parser = yml_parser.YamlParser(profiles_path)
            profile_yml = parser.parse()
            return profile_yml
        
        # If template isn't found, return an empty dictionary
        except jinja2.exceptions.TemplateNotFound:
            return {}
        
        # Raise all other exceptions
        except Exception as e:
            raise e
    

    def exec(self,
        globals_dict: Dict[Any, Any]
    ) -> None:
        # This object is only instantiated within a project directory
        globals_dict['__file__'] = 'prism_project.py'
        exec(self.prism_project_py_str, globals_dict)


# EOF