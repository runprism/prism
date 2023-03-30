"""
Unit testing for functions in YamlParser class. The YamlParser is called in both the
compile task (for prism_project.py) and the run task (for profile YML).

Table of Contents:
- Imports
- Test case directory and paths
- Instances of compile and run task to call functions
- Test case class definition
"""


###########
# Imports #
###########

# Standard library imports
import jinja2
import os
from pathlib import Path
import unittest

# Prism imports
import prism.exceptions
from prism.parsers import yml_parser


#################################
# Test case directory and paths #
#################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
PROJECT_YML_TEST_CASES = Path(TEST_CASE_WKDIR) / 'test_jinja_functions'

# Test cases related to imports
CONCAT = PROJECT_YML_TEST_CASES / 'concat.yml'
ENV_DOESNT_EXIST = PROJECT_YML_TEST_CASES / 'env_doesnt_exist.yml'
ENV_EXISTS = PROJECT_YML_TEST_CASES / 'env_exists.yml'
PARENT = PROJECT_YML_TEST_CASES / 'parent.yml'
WKDIR = PROJECT_YML_TEST_CASES / 'wkdir.yml'
PATH_STUFF = PROJECT_YML_TEST_CASES / 'path_stuff.yml'

# List of all test case .yml files
ALL_TEST_CASE_YML_FILES = [
    CONCAT,
    ENV_EXISTS,
    PARENT,
    WKDIR,
    PATH_STUFF,
]


##############################
# Test case class definition #
##############################

class TestJinjaFunctions(unittest.TestCase):

    def _load_profile_yml(self,
        profiles_path: Path
    ) -> dict:
        """
        Load profile YML file

        args:
            profiles_path: path to profile YML
        returns:
            profile_yml: profile YML file represented as a dict
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

    ###################################################
    # Common test cases across prism_project.py files #
    ###################################################

    def test_load_yml(self):
        """
        Test that test case .yml files can be loaded
        """
        for file in ALL_TEST_CASE_YML_FILES:
            self._load_profile_yml(file)

    #######################################
    # Test cases for individual functions #
    #######################################

    def test_env_exists(self):
        """
        env() function returns an environment variable
        """
        yml = self._load_profile_yml(ENV_EXISTS)
        expected_user = "/bin/bash"
        actual_user = yml['profile_name']['adapters']['snowflake_adapter_name_here']['user']  # noqa: E501
        self.assertEqual(expected_user, actual_user)

    def test_env_doesnt_exist(self):
        """
        env() function returns a blank string if the environment variable doesn't exist
        """
        with self.assertRaises(prism.exceptions.EnvironmentVariableNotFoundException) as cm:  # noqa: E501
            _ = self._load_profile_yml(ENV_DOESNT_EXIST)
        expected_msg = "environment variable `THIS_SHOULD_FAIL` not found"
        self.assertEqual(expected_msg, str(cm.exception))

    def test_concat(self):
        """
        concat() function concatenates two strings correctly
        """
        yml = self._load_profile_yml(CONCAT)
        expected_yml_keys = ['jinja_commands', 'base_etl']
        self.assertEqual(set(expected_yml_keys), set(list(yml.keys())))

    def test_parent(self):
        """
        parent() function returns the parent directory for the inputted path
        """
        yml = self._load_profile_yml(PARENT)
        expected_profiles_dir = PARENT.parent.parent
        actual_profiles_dir = yml['profile_name']['adapters']['dbt']['profiles_dir']
        self.assertEqual(str(expected_profiles_dir), actual_profiles_dir)

    def test_wkdir(self):
        """
        wkdir() function returns the current directory
        """
        yml = self._load_profile_yml(WKDIR)
        expected_profiles_dir = WKDIR.parent
        actual_profiles_dir = yml['profile_name']['adapters']['dbt']['profiles_dir']
        self.assertEqual(str(expected_profiles_dir), actual_profiles_dir)

    def test_path(self):
        """
        Path(__file__) returns the path of the YAML file and can be manipulated like
        Pathlib's Path object.
        """
        yml = self._load_profile_yml(PATH_STUFF)
        expected_project_dir = PATH_STUFF
        expected_profiles_dir = PATH_STUFF.parent
        actual_project_dir = yml['profile_name']['adapters']['dbt']['project_dir']
        actual_profiles_dir = yml['profile_name']['adapters']['dbt']['profiles_dir']
        self.assertEqual(str(expected_project_dir), str(actual_project_dir))
        self.assertEqual(str(expected_profiles_dir), str(actual_profiles_dir))
