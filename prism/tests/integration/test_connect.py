"""
Integration tests.

Table of Contents:
- Imports
- Test case directory and paths
- Expected events / results
- Test case class definition
"""

#############
## Imports ##
#############

# Standard library imports
import os
from pathlib import Path
import re
import shutil
import unittest
import yaml

# Prism imports
import prism.cli.base
from prism.main import main
import prism.logging
import prism.tests.integration.integration_test_class as integration_test_class


###################################
## Test case directory and paths ##
###################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TEST_PROJECTS = Path(TEST_CASE_WKDIR) / 'test_projects'


###############################
## Expected events / results ##
###############################

# Header events constant all task runs
header_events = [
    'SeparatorEvent',
    'TaskRunEvent',
    'CurrentProjectDirEvent'
]

# The events associated with the `connect` task not depend on the value of `--type`. All successful runs should result
# in the same events being fired.
connect_task_successful_expected_events = header_events + [
    'EmptyLineEvent',
    'SettingUpProfileEvent',
    'EmptyLineEvent',
    'TaskSuccessfulEndEvent',
    'SeparatorEvent'
]

# Events associated with invalid profile type
connect_task_invalid_expected_events = header_events + [
    'EmptyLineEvent',
    'InvalidProfileType',
    'SeparatorEvent'
]

# Snowflake profile as a dict
expected_snowflake_dict = {
    'profile_name': {
        'adapters': {
            'snowflake': {
                'user': None,
                'password': None,
                'account': None,
                'role': None,
                'warehouse': None,
                'database': None,
                'schema': None
            }
        }
    }
}

# PySpark profile as a dict
expected_pyspark_dict = {
    'profile_name': {
        'adapters': {
            'pyspark': {
                'alias': 'spark',
                'loglevel': 'WARN',
                'config': {
                    'spark.driver.cores': None,
                    'spark.driver.memory': None,
                    'spark.driver.memoryOverhead': None,
                    'spark.executor.cores': None,
                    'spark.executor.memory': None,
                    'spark.executor.memoryOverhead': None,
                    'spark.executor.instances': None,
                    'spark.task.cpus': None,
                    'spark.sql.broadcastTimeout': None
                }
            }
        }
    }
}

# Snowflake + PySpark profile
expected_snowflake_pyspark_dict = {
    'profile_name': {
        'adapters': {
            'snowflake': {
                'user': None,
                'password': None,
                'account': None,
                'role': None,
                'warehouse': None,
                'database': None,
                'schema': None
            },
            'pyspark': {
                'alias': 'spark',
                'loglevel': 'WARN',
                'config': {
                    'spark.driver.cores': None,
                    'spark.driver.memory': None,
                    'spark.driver.memoryOverhead': None,
                    'spark.executor.cores': None,
                    'spark.executor.memory': None,
                    'spark.executor.memoryOverhead': None,
                    'spark.executor.instances': None,
                    'spark.task.cpus': None,
                    'spark.sql.broadcastTimeout': None
                }
            }
        }
    }
}


################################
## Test case class definition ##
################################

class TestConnectIntegration(integration_test_class.IntegrationTestCase):

    def _remove_profile_yml(self, wkdir):
        """
        Remove the profile.yml file, if it exists
        """
        if Path(wkdir / 'profile.yml').is_file():
            os.unlink(Path(wkdir / 'profile.yml'))
    

    def _profile_yml_as_dict(self, wkdir):
        """
        Open the profile.yml file as a dict
        """
        with open(Path(wkdir / 'profile.yml'), 'r') as f:
            yml_dict = yaml.safe_load(f)
        f.close()
        return yml_dict


    def _test_profile_successfully_created(self, wkdir, task_run_return_result):
        """
        Run a standard set of checks to confirm whether a profile was created successfully
        """
        results = task_run_return_result.get_results()
        self.assertTrue(Path(wkdir / 'profile.yml').is_file())
        self.assertEqual(' | '.join(connect_task_successful_expected_events), results)


    def test_snowflake(self):
        """
        `prism connect` using Snowflake proflie
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '006_simple_project_with_profile'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_profile_yml(wkdir)

        # Execute command
        args = ['connect', '--type', 'snowflake']
        connect_run = self._run_prism(args)
        self._test_profile_successfully_created(wkdir, connect_run)

        # See contents of profile.yml
        yml_dict = self._profile_yml_as_dict(wkdir)
        self.assertEqual(expected_snowflake_dict, yml_dict)

        # Set up wkdir for next test case
        self._set_up_wkdir()


    def test_pyspark(self):
        """
        `prism connect` using PySpark profile
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '006_simple_project_with_profile'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_profile_yml(wkdir)

        # Execute command
        args = ['connect', '--type', 'pyspark']
        connect_run = self._run_prism(args)
        self._test_profile_successfully_created(wkdir, connect_run)

        # See contents of profile.yml
        yml_dict = self._profile_yml_as_dict(wkdir)
        self.assertEqual(expected_pyspark_dict, yml_dict)

        # Set up wkdir for next test case
        self._set_up_wkdir()


    def test_add_diff_profile_to_existing_one(self):
        """
        `prism connect` using Snowflake + PySpark profile
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '006_simple_project_with_profile'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_profile_yml(wkdir)

        # Execute command for Snowflake
        # -----------------------------
        args = ['connect', '--type', 'snowflake']
        connect_run = self._run_prism(args)
        self._test_profile_successfully_created(wkdir, connect_run)

        # See contents of profile.yml
        yml_dict = self._profile_yml_as_dict(wkdir)
        self.assertEqual(expected_snowflake_dict, yml_dict)


        # Add Snowflake profile
        # ---------------------
        args = ['connect', '--type', 'pyspark']
        connect_run = self._run_prism(args)
        self._test_profile_successfully_created(wkdir, connect_run)

        # See contents of profile.yml
        yml_dict = self._profile_yml_as_dict(wkdir)
        self.assertEqual(expected_snowflake_pyspark_dict, yml_dict)

        # Set up wkdir for next test case
        self._set_up_wkdir()

    
    def test_add_same_profile_to_existing_one(self):
        """
        `prism connect` using Snowflake + PySpark profile
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '006_simple_project_with_profile'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_profile_yml(wkdir)

        # Execute command for Snowflake
        # -----------------------------
        args = ['connect', '--type', 'snowflake']
        connect_run = self._run_prism(args)
        self._test_profile_successfully_created(wkdir, connect_run)

        # See contents of profile.yml
        yml_dict = self._profile_yml_as_dict(wkdir)
        self.assertEqual(expected_snowflake_dict, yml_dict)


        # Add EMF profile
        # ---------------
        args = ['connect', '--type', 'snowflake']
        connect_run = self._run_prism(args)
        connect_run_results = connect_run.get_results()
        expected_results = ' | '.join(header_events + [
            'EmptyLineEvent',
            'SettingUpProfileEvent',
            'EmptyLineEvent',
            'PrismExceptionErrorEvent',
            'SeparatorEvent'
        ])
        self.assertEqual(expected_results, connect_run_results)


    def test_invalid_type(self):
        """
        `prism connect` using an invalid profile type
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '006_simple_project_with_profile'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_profile_yml(wkdir)

        # Execute command with a non-null, invalid type
        args = ['connect', '--type', 'dummy']
        connect_run = self._run_prism(args)
        connect_run_results = connect_run.get_results()
        self.assertEqual(' | '.join(connect_task_invalid_expected_events), connect_run_results)

        # Execute command with a Null
        args = ['connect', '--type', '']
        connect_run = self._run_prism(args)
        connect_run_results = connect_run.get_results()
        self.assertEqual(' | '.join(connect_task_invalid_expected_events), connect_run_results)

        # Set up wkdir for next test case
        self._set_up_wkdir()


    def test_no_prism_project_py(self):
        """
        `prism connect` when there is no prism_project.py file
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '002_no_project_py'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_profile_yml(wkdir)

        # Execute command
        args = ['connect', '--type', 'snowflake']
        connect_run = self._run_prism(args)
        connect_run_results = connect_run.get_results()
        expected_results = ' | '.join([
            'SeparatorEvent',
            'TaskRunEvent',
            'EmptyLineEvent',
            'ProjectPyNotFoundEvent',
            'SeparatorEvent'
        ])
        print(connect_run_results)
        self.assertEqual(expected_results, connect_run_results)

        # Set up wkdir for next test case
        self._set_up_wkdir()



        
# EOF