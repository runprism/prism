"""
Prism Project class

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Standard library imports
import os
import ast
import astor
import jinja2
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

# Prism-specific imports
import prism.exceptions
from prism.logging import fire_console_event
import prism.logging
from prism.parsers import yml_parser
from prism.profiles import profile


####################
# Class definition #
####################

class PrismProject:
    """
    Class to represent configuration files (prism_project.py and profile.yml)
    """

    def __init__(self,
        project_dir: Path,
        user_context: Dict[str, Any],
        which: str,
        filename: str ='prism_project.py'
    ):
        self.project_dir = project_dir
        self.user_context = user_context
        self.which = which
        self.filename = filename

        # Load prism_project.py as a string
        self.prism_project_py_str = self.load_prism_project_py(
            self.project_dir, self.filename
        )
        # Keep track of any adjustments made via configurations
        self.prism_project_py_str_adjusted: Optional[str] = None
    
    def load_prism_project_py(self,
        project_dir: Path,
        filename: str = "prism_project.py"
    ) -> str:
        """
        Load the prism_project.py file as a string

        args:
            project_dir: project directory
            filename: name of prism_project.py file; default is "prism_project.py"
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

    def exec(self,
        run_context: Dict[Any, Any]
    ):
        """
        Execute project
        """
        run_context['__file__'] = str(self.project_dir / self.filename)
        exec(self.prism_project_py_str, run_context)

        # Override `prism_project.py` with context vars
        for k, v in self.user_context.items():
            run_context[k] = v

    def setup(self):
        """
        Set up prism project. This should always be directly after object instantiation
        (except for in testing).
        """       
        # Execute
        self.run_context = prism.constants.CONTEXT.copy()
        self.exec(self.run_context)

        # ------------------------------------------------------------------------------
        # Compiled sys.path config
        try:
            self.sys_path_config = self.run_context['SYS_PATH_CONF']

            # If project directory not in sys_path_config, throw a warning
            if str(self.project_dir) not in [str(p) for p in self.sys_path_config]:  # noqa: E501
                prism.logging.fire_console_event(
                    prism.logging.ProjectDirNotInSysPath(), [], log_level='warn'
                )

        # Fire a warning, even if the user specified `quietly`
        except KeyError:
            prism.logging.fire_console_event(
                prism.logging.SysPathConfigWarningEvent(), [], log_level='warn'
            )
            self.sys_path_config = [self.project_dir]

        # ------------------------------------------------------------------------------
        # Thread count

        self.thread_count = self.get_thread_count(self.run_context)
        
        # ------------------------------------------------------------------------------
        # Profile name, profiles dir, and profiles path

        self.profile_name = self.get_profile_name(self.run_context)
        self.profiles_dir = self.get_profiles_dir(self.run_context)

        # If we're creating the project as part of the `connect` task, we don't need to
        # generate the adapters; we only need to grab the profiles directory. If the
        # profiles dir isn't specified, default to the project dir.
        if self.which == "connect":
            fire_console_event(
                prism.logging.ProfileDirWarningEvent(),
                [],
                0.01,
                'warn'
            )
            self.profiles_dir = self.project_dir
        
        # As of now, the only other tasks that sets up the project is the `run` and
        # `spark-submit` tasks. For these, we do need to generate the adapters.
        else:
            # If the profiles dir isn't specified, only raise a warning if the profile
            # name is non-empty.
            if self.profile_name != "" and self.profiles_dir is None:
                fire_console_event(
                    prism.logging.ProfileDirWarningEvent(),
                    [],
                    0.01,
                    'warn'
                )
                self.profiles_dir = self.project_dir
            self.profiles_path = self.profiles_dir / 'profile.yml' if self.profiles_dir is not None else None  # noqa: E501

            # Do all the other profile-related stuff
            self.profile_yml = self.load_profile_yml(self.profiles_path)
            self.profile = profile.Profile(self.profile_yml, self.profile_name)
            self.profile.generate_adapters()
            self.adapters_object_dict = self.profile.get_adapters_obj_dict()

    def adjust_prism_py_with_config(self,
        config_dict: Dict[str, Any]
    ):
        """
        Overwrite any variables in prism_project.py with those in config_dict.

        args:
            config_dict: configuration dictionary with var --> value mappings
            globals_dict: namespace dictionary
        returns:
            None
        """
        self.prism_project_py_str_adjusted = self.prism_project_py_str + '\n'
        for k, v in config_dict.items():
            if isinstance(v, str):
                self.prism_project_py_str_adjusted += f"{k} = '{v}'"
            else:
                self.prism_project_py_str_adjusted += f"{k} = {v}"

        # Re-write the prism_project.py file -- we undo this later
        with open(Path(self.project_dir / 'prism_project.py'), 'w') as f:
            f.write(self.prism_project_py_str_adjusted)
        f.close()

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
                            if sub_elem.id == var:
                                num_assignments += 1
                        else:
                            continue

                # Otherwise, check name of assignment var
                else:
                    if target.id == var:
                        num_assignments += 1
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
            raise prism.exceptions.InvalidProjectPyException(
                message="`prism_project.py` is undefined"
            )

        # If multiple assignments, throw an error
        num_assigments = self.num_var_assignments_in_file(python_file, var)
        if num_assigments > 1:
            raise prism.exceptions.InvalidProjectPyException(
                message=f"multiple assignments for `{var}` in `prism_project.py`"
            )

        # Parse the Python file. We will wrap this function in an event manager, so no
        # need to handle exceptions at the moment.
        ast_data = ast.parse(python_file)
        for elem in ast_data.body:
            if isinstance(elem, ast.Assign):
                if len(elem.targets) == 1:
                    if getattr(elem.targets[0], "id", "") == var:

                        # If the element is a list, then iterate through the elements
                        # and return the string value. We'll execute it later.
                        if isinstance(elem.value, ast.List):
                            items = []
                            for attr in elem.value.elts:
                                python_greater_than_39 = prism.constants.PYTHON_VERSION.major == 3 and prism.constants.PYTHON_VERSION.minor >= 9  # noqa: E501
                                if prism.constants.PYTHON_VERSION.major > 3 or python_greater_than_39:  # noqa: E501

                                    # mypy thinks ast doesn't have an unparse method,
                                    # but this is fine.
                                    items.append(ast.unparse(attr))  # type: ignore
                                else:
                                    items.append(
                                        re.sub('\n$', '', astor.to_source(attr))
                                    )
                            return items

                        elif isinstance(elem.value, ast.Attribute):
                            return ast.unparse(elem.value)
                        else:
                            return ast.literal_eval(elem.value)
        return None

    def get_profile_name(self,
        run_context: Dict[Any, Any]
    ) -> str:
        """
        Get profile name from the run context

        args:
            run context
        returns:
            profile_name
        """
        profile_name = run_context.get("PROFILE", None)
        if profile_name is None:
            return ""
        if not isinstance(profile_name, str):
            return ""
        return profile_name
    
    def get_profiles_dir(self,
        run_context: Dict[Any, Any]
    ) -> Optional[Path]:
        """
        Get profile path from current run context

        args:
            prism_project_py: prism_project.py file represented as string
        returns:
            profile path
        """
        profiles_dir = run_context.get('PROFILES_DIR', None)
        if profiles_dir is None:
            return None
        if not (isinstance(profiles_dir, str) or isinstance(profiles_dir, Path)):
            return None
        return Path(profiles_dir)

    def get_sys_path_config(self,
        prism_project_py: str
    ) -> List[str]:
        """
        Get sys.path configuration from prism_project.py file

        args:
            prism_project_py: prism_project.py file represented as string
        returns:
            sys_path configuration; this should be a list
        """
        sys_path_config = self.safe_eval_var_from_file(
            prism_project_py, 'SYS_PATH_CONF'
        )
        if sys_path_config is None:
            fire_console_event(
                prism.logging.SysPathConfigWarningEvent(),
                [],
                0.01,
                'warn'
            )
        if not isinstance(sys_path_config, list):
            return []
        return sys_path_config

    def get_thread_count(self,
        run_context: Dict[Any, Any]
    ) -> int:
        """
        Get thread count from prism_project.py file. If thread count is not
        specified, then default to 1.

        args:
            prism_project_py: prism_project.py file represented as string
        returns:
            profile_name
        """
        thread_count = run_context.get("THREADS", None)
        if thread_count is None:
            fire_console_event(
                prism.logging.ThreadsWarningEvent(),
                [],
                0.01,
                'warn'
            )
            return 1
        if not isinstance(thread_count, int):
            msg_list = [
                f'invalid value `THREADS = {thread_count}`',
                'must be an integer'
            ]
            raise prism.exceptions.InvalidProjectPyException(
                message='\n'.join(msg_list)
            )
        if thread_count < 1:
            return 1
        return thread_count

    def load_profile_yml(self,
        profiles_path: Optional[Path]
    ) -> Dict[Any, Any]:
        """
        Load profile.yml file

        args:
            profiles_path: path to profile.yml
        returns:
            profile_yml: profile.yml file represented as a dict
        """
        # If no profile path is specified, return None
        if profiles_path is None:
            return {}

        # Otherwise, try and load a template
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
