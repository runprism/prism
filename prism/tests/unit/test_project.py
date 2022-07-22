"""
Unit testing for functions for parsing the prism_project.py file. These functions are called by instances of the run 
task.

Table of Contents:
- Imports
- Test case directory and paths
- Run task instance to call functions
- Test case class definition
"""

#############
## Imports ##
#############

# Standard library imports
import os
from pathlib import Path
import unittest

# Prism imports
import prism.exceptions
from prism.infra import project


###################################
## Test case directory and paths ##
###################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
PRISM_PROJECT_PY_TEST_CASES = Path(TEST_CASE_WKDIR) / 'test_prism_project_py'

# Test cases related to imports
MULTIPLE_PROFILES = 'multiple_profiles.py'
NULL_PROFILE = 'null_profile.py'
NON_NULL_PROFILE = 'non_null_profile.py'
NO_PROFILE = 'no_profile.py'

# List of all test case .yml files
ALL_TEST_CASE_FILES = [
    MULTIPLE_PROFILES,
    NULL_PROFILE,
    NON_NULL_PROFILE,
    NO_PROFILE
]


################################
## Test case class definition ##
################################

class TestPrismProject(unittest.TestCase):

    def test_load_all_tests(self):
        """
        Confirm that all test *.py files can be loaded without error
        """
        for filename in ALL_TEST_CASE_FILES:
            prism_project = project.PrismProject(project_dir=PRISM_PROJECT_PY_TEST_CASES, profiles_path=None, env="local", which="run", filename=filename)
        

    def test_non_null_profile(self):
        """
        The run task uses the AST module to extract the profile variable from the prism_project.py file without
        executing it. Confirm that this module extracts a non-null profile correctly.
        """
        prism_project = project.PrismProject(project_dir=PRISM_PROJECT_PY_TEST_CASES, profiles_path=None, env="local", which="run", filename=NON_NULL_PROFILE)
        prism_project_py_str = prism_project.prism_project_py_str
        profile = prism_project.safe_eval_var_from_file(prism_project_py_str, 'profile')
        expected_profile = 'this_is_a_test!!!'
        self.assertEqual(expected_profile, profile)
    

    def test_null_profile(self):
        """
        Confirm that the run task correctly extracts the profile variable when it is set to 'None'
        """
        prism_project = project.PrismProject(project_dir=PRISM_PROJECT_PY_TEST_CASES, profiles_path=None, env="local", which="run", filename=NULL_PROFILE)
        prism_project_py_str = prism_project.prism_project_py_str
        profile = prism_project.safe_eval_var_from_file(prism_project_py_str, 'profile')
        self.assertTrue(profile is None)
    

    def test_no_profile(self):
        """
        Confirm that the run task correctly returns None when the inputted var (which will almost always be `profile`)
        is missing
        """
        prism_project = project.PrismProject(project_dir=PRISM_PROJECT_PY_TEST_CASES, profiles_path=None, env="local", which="run", filename=NO_PROFILE)
        prism_project_py_str = prism_project.prism_project_py_str
        profile = prism_project.safe_eval_var_from_file(prism_project_py_str, 'profile')
        self.assertTrue(profile is None)
    

    def test_multiple_profiles(self):
        """
        Confirm that the run task throws an error if there are multiple assignments for the inputted var (which will
        almost always be `profile`)
        """
        prism_project = project.PrismProject(project_dir=PRISM_PROJECT_PY_TEST_CASES, profiles_path=None, env="local", which="run", filename=MULTIPLE_PROFILES)
        prism_project_py_str = prism_project.prism_project_py_str
        with self.assertRaises(prism.exceptions.InvalidProjectPyException) as cm:
            profile = prism_project.safe_eval_var_from_file(prism_project_py_str, 'profile')
        expected_msg = 'multiple assignments for `profile` in `prism_project.py`'
        self.assertEqual(expected_msg, str(cm.exception))


# EOF