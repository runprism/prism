"""
Unit testing for functions for parsing the prism_project.py file. These functions are
called by instances of the run task.

Table of Contents:
- Imports
- Test case directory and paths
- Run task instance to call functions
- Test case class definition
"""

###########
# Imports #
###########

# Standard library imports
import os
from pathlib import Path
import unittest

# Prism imports
from prism.infra import project
import prism.exceptions
from prism.infra.sys_path import SysPathEngine


#################################
# Test case directory and paths #
#################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
PRISM_PROJECT_PY_TEST_CASES = Path(TEST_CASE_WKDIR) / 'test_prism_project_py'

# Test cases related to imports
MULTIPLE_PROFILES = 'multiple_profiles.py'
NULL_PROFILE = 'null_profile.py'
NON_NULL_PROFILE = 'non_null_profile.py'
NO_PROFILE = 'no_profile.py'
TRIGGERS_NORMAL = 'triggers_normal.py'
ON_FAILURE_TRIGGERS_ONLY = 'on_failure_triggers_only.py'
ON_SUCCESS_TRIGGERS_ONLY = 'on_success_triggers_only.py'
BAD_TRIGGER_KEY = 'bad_trigger_key.py'

# List of all test case .yml files
ALL_TEST_CASE_FILES = [
    MULTIPLE_PROFILES,
    NULL_PROFILE,
    NON_NULL_PROFILE,
    NO_PROFILE
]


##############################
# Test case class definition #
##############################

class TestPrismProject(unittest.TestCase):

    def test_load_all_tests(self):
        """
        Confirm that all test *.py files can be loaded without error
        """
        for filename in ALL_TEST_CASE_FILES:
            project.PrismProject(
                project_dir=PRISM_PROJECT_PY_TEST_CASES,
                user_context={},
                which="run",
                filename=filename
            )

    def test_non_null_profile(self):
        """
        The run task uses the AST task to extract the profile variable from the
        prism_project.py file without executing it. Confirm that this task extracts a
        non-null profile correctly.
        """
        prism_project = project.PrismProject(
            project_dir=PRISM_PROJECT_PY_TEST_CASES,
            user_context={},
            which="run",
            filename=NON_NULL_PROFILE
        )
        prism_project_py_str = prism_project.prism_project_py_str
        profile = prism_project.safe_eval_var_from_file(
            prism_project_py_str, 'PROFILE', {}
        )
        expected_profile = 'this_is_a_test!!!'
        self.assertEqual(expected_profile, profile)

        # Remove logs.log
        if Path(Path(TEST_CASE_WKDIR) / 'logs.log').is_file():
            os.unlink(Path(TEST_CASE_WKDIR) / 'logs.log')

    def test_null_profile(self):
        """
        Confirm that the run task correctly extracts the profile variable when it is set
        to 'None'
        """
        prism_project = project.PrismProject(
            project_dir=PRISM_PROJECT_PY_TEST_CASES,
            user_context={},
            which="run",
            filename=NULL_PROFILE
        )
        prism_project_py_str = prism_project.prism_project_py_str
        profile = prism_project.safe_eval_var_from_file(
            prism_project_py_str, 'PROFILE', {}
        )
        self.assertTrue(profile is None)

    def test_no_profile(self):
        """
        Confirm that the run task correctly returns None when the inputted var (which
        will almost always be `profile`) is missing
        """
        prism_project = project.PrismProject(
            project_dir=PRISM_PROJECT_PY_TEST_CASES,
            user_context={},
            which="run",
            filename=NO_PROFILE
        )
        prism_project_py_str = prism_project.prism_project_py_str
        profile = prism_project.safe_eval_var_from_file(
            prism_project_py_str, 'PROFILE', {}
        )
        self.assertTrue(profile is None)

    def test_multiple_profiles(self):
        """
        If a var is defined multiple times, then the last definition will take
        precedence.
        """
        prism_project = project.PrismProject(
            project_dir=PRISM_PROJECT_PY_TEST_CASES,
            user_context={},
            which="run",
            filename=MULTIPLE_PROFILES
        )
        prism_project_py_str = prism_project.prism_project_py_str
        profile = prism_project.safe_eval_var_from_file(
            prism_project_py_str, 'PROFILE', {}
        )
        self.assertEqual(profile, "test2")

    def test_normal_triggers(self):
        """
        Trigger directory and triggers are defined as expected
        """
        run_context = {}
        prism_project = project.PrismProject(
            project_dir=PRISM_PROJECT_PY_TEST_CASES,
            user_context={},
            which="run",
            filename=TRIGGERS_NORMAL
        )
        engine = SysPathEngine(run_context)
        prism_project.exec(run_context, engine)

        # Triggers directory
        triggers_yml_path = prism_project.get_triggers_yml_path(run_context)
        self.assertEqual(
            str(triggers_yml_path),
            str(PRISM_PROJECT_PY_TEST_CASES / 'triggers.yml')
        )

        # Triggers
        triggers = prism_project.get_triggers(run_context)
        expected_triggers = {
            "on_success": ['test_fn'],
            "on_failure": ['test_fn'],
        }
        self.assertEqual(triggers, expected_triggers)

    def test_on_failure_triggers_only(self):
        """
        on_success triggers is an empty list when not defined
        """
        run_context = {}
        prism_project = project.PrismProject(
            project_dir=PRISM_PROJECT_PY_TEST_CASES,
            user_context={},
            which="run",
            filename=ON_FAILURE_TRIGGERS_ONLY
        )
        engine = SysPathEngine(run_context)
        prism_project.exec(run_context, engine)

        # Triggers
        triggers = prism_project.get_triggers(run_context)
        expected_triggers = {
            "on_success": [],
            "on_failure": ['test_fn'],
        }
        self.assertEqual(triggers, expected_triggers)

    def test_on_success_triggers_only(self):
        """
        on_failure triggers is an empty list when not defined
        """
        run_context = {}
        prism_project = project.PrismProject(
            project_dir=PRISM_PROJECT_PY_TEST_CASES,
            user_context={},
            which="run",
            filename=ON_SUCCESS_TRIGGERS_ONLY
        )
        engine = SysPathEngine(run_context)
        prism_project.exec(run_context, engine)

        # Triggers
        triggers = prism_project.get_triggers(run_context)
        expected_triggers = {
            "on_success": ['test_fn'],
            "on_failure": [],
        }
        self.assertEqual(triggers, expected_triggers)

    def test_bad_trigger_key(self):
        """
        on_failure triggers is an empty list when not defined
        """
        run_context = {}
        prism_project = project.PrismProject(
            project_dir=PRISM_PROJECT_PY_TEST_CASES,
            user_context={},
            which="run",
            filename=BAD_TRIGGER_KEY
        )
        engine = SysPathEngine(run_context)
        prism_project.exec(run_context, engine)

        # Triggers
        with self.assertRaises(prism.exceptions.InvalidProjectPyException) as cm:
            prism_project.get_triggers(run_context)
        expected_msg = 'invalid key `this_key_should_not_exist` in TRIGGERS dictionary'
        self.assertEqual(str(cm.exception), expected_msg)
