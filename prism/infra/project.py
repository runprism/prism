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
        filename: str = 'prism_project.py'
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

        self.sys_path_config = self.get_sys_path_config(self.run_context)

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

        # ------------------------------------------------------------------------------
        # Triggers directory and triggers

        self.triggers_dir = self.get_triggers_dir(self.run_context)
        triggers = self.get_triggers(self.run_context)
        if triggers is None:
            self.on_success_triggers, self.on_failure_triggers = [], []
        else:
            self.on_success_triggers = triggers["on_success"]
            self.on_failure_triggers = triggers["on_failure"]

    def ast_unparse(self, elt: Any):
        """
        Some versions of Python do not support the `ast.unparse` method. This function
        checks the Python version and determines the appropriate function to call.

        args:
            elt: AST element
        returns:
            unparsed element
        """
        python_greater_than_39 = prism.constants.PYTHON_VERSION.major == 3 and prism.constants.PYTHON_VERSION.minor >= 9  # noqa: E501
        if prism.constants.PYTHON_VERSION.major > 3 or python_greater_than_39:  # noqa: E501
            # mypy thinks ast doesn't have an unparse method,
            # but this is fine.
            return ast.unparse(elt)  # type: ignore
        else:
            return re.sub('\n$', '', astor.to_source(elt))

    def safe_eval_var_from_file(self,
        python_file: str,
        var: str,
        context: Dict[Any, Any],
    ) -> Any:
        """
        Get `var` from `python_file`

        args:
            python_file: Python file represented as a str
            var: variable of interest
            context: context dictionary
        returns:
            value of first `var` assignment in `python_file`
        """

        # If `python_file` is None, throw an error
        if python_file is None:
            raise prism.exceptions.InvalidProjectPyException(
                message="`prism_project.py` is undefined"
            )

        # Iterate through ast.Assign objects. For each Assign object, unparse and check
        # if the var is defined within it. If it is, then execute and return the
        # executed value.
        var_value = None
        ast_data = ast.parse(python_file)
        for elem in ast.walk(ast_data):
            if isinstance(elem, ast.Assign):
                elem_code = self.ast_unparse(elem)
                if var in elem_code:
                    exec(elem_code, context)
                    var_value = context[var]
        return var_value

    def get_profile_name(self,
        run_context: Dict[Any, Any]
    ) -> str:
        """
        Get profile name from the run context

        args:
            run_context: dictionary with run context variables
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
            run_context: dictionary with run context variables
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
        run_context: Dict[Any, Any]
    ) -> List[Path]:
        """
        Get sys.path configuration from the run context

        args:
            run_context: dictionary with run context variables
        returns:
            sys_path configuration; this should be a list
        """
        sys_path_config = run_context.get('SYS_PATH_CONF', None)
        if sys_path_config is None:
            fire_console_event(
                prism.logging.SysPathConfigWarningEvent(),
                [],
                0.01,
                'warn'
            )
            return [self.project_dir]
        if not isinstance(sys_path_config, list):
            raise prism.exceptions.RuntimeException(
                message='`SYS_PATH_CONF` must be a list'
            )

        # If the project directory is not in the sys.path config, throw a warning and
        # add it.
        if str(self.project_dir) not in [str(s) for s in sys_path_config]:
            prism.logging.fire_console_event(
                prism.logging.ProjectDirNotInSysPath(), [], log_level='warn'
            )
            sys_path_config.insert(0, self.project_dir)
        return [Path(s) for s in sys_path_config]

    def get_thread_count(self,
        run_context: Dict[Any, Any]
    ) -> int:
        """
        Get thread count from prism_project.py file. If thread count is not
        specified, then default to 1.

        args:
            run_context: dictionary with run context variables
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

    def get_triggers_dir(self,
        run_context: Dict[Any, Any]
    ) -> Optional[Path]:
        """
        Get triggers path from current run context. This doesn't have to be specified.

        args:
            run_context: dictionary with run context variables
        returns:
            triggers path
        """
        triggers = run_context.get('TRIGGERS_DIR', None)
        if triggers is None:
            return None
        if not (isinstance(triggers, str) or isinstance(triggers, Path)):
            return None
        return Path(triggers)

    def get_triggers(self,
        run_context: Dict[Any, Any]
    ) -> Optional[Dict[str, List[str]]]:
        """
        Get triggers from current run context. This doesn't have to be specified.

        args:
            run_context: dictionary with run context variables
        returns:
            triggers
        """
        triggers = run_context.get("TRIGGERS", None)
        if triggers is None:
            return None

        # Triggers must be specified as a dictionary
        if not isinstance(triggers, dict):
            raise prism.exceptions.InvalidProjectPyException(
                message=f'invalid value `TRIGGERS = {triggers}`, must be a dictionary'
            )

        # There can only be two types of triggers: on_success and on_failure
        trigger_keys = list(triggers.keys())
        expected_keys = ['on_success', 'on_failure']
        for k in trigger_keys:
            if k not in expected_keys:
                raise prism.exceptions.InvalidProjectPyException(
                    message=f'invalid key `{k}` in TRIGGERS dictionary'
                )

        # on_success and on_failure triggers should be a list of strings
        success_triggers = triggers['on_success'] if 'on_success' in trigger_keys else []  # noqa: E501
        failure_triggers = triggers['on_failure'] if 'on_failure' in trigger_keys else []  # noqa: E501
        if not (
            isinstance(success_triggers, list)
            and isinstance(failure_triggers, list)  # noqa: W503
            and all([isinstance(t, str) for t in success_triggers])  # noqa: W503
            and all([isinstance(t, str) for t in failure_triggers])  # noqa: W503
        ):
            raise prism.exceptions.InvalidProjectPyException(
                message='invalid TRIGGERS dictionary, both `on_success` and `on_failure` values must be a list of strings'  # noqa: E501
            )

        return {
            "on_success": success_triggers,
            "on_failure": failure_triggers,
        }
