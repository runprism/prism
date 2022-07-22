"""
Integration tests for `init` task,

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
import shutil
import unittest

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


################################
## Test case class definition ##
################################

class TestInitIntegration(integration_test_class.IntegrationTestCase):


    def test_init(self):
        """
        `prism init`
        """
        
        # Set working directory
        os.chdir(TEST_PROJECTS)

        # Remove folder '001_init' if it already exists
        init_path = Path(TEST_PROJECTS) / '001_init'
        if init_path.is_dir():
            shutil.rmtree(init_path)

        # Execute init command
        args = ['init', '--project-name', '001_init']
        init_run = self._run_prism(args)
        init_run_results = init_run.get_results()
        
        # Since project has not been created, the events should align with a successful task creation
        expected_events = ' | '.join([
            'SeparatorEvent',
            'TaskRunEvent',
            'EmptyLineEvent',
            'CreatingProjectDirEvent',
            'EmptyLineEvent',
            'InitSuccessfulEvent',
            'SeparatorEvent',
        ])
        self.assertEqual(expected_events, init_run_results)
        self.assertTrue(init_path.is_dir())
        self._is_valid_project(init_path)

        # Try creating the project again. This time, we should see a `ProjectAlreadyExistsEvent`
        os.chdir(TEST_PROJECTS)
        init_run_already_exists = self._run_prism(args)
        init_run_already_exists_results = init_run_already_exists.get_results()
        expected_events_already_exists = ' | '.join([
            'SeparatorEvent',
            'TaskRunEvent',
            'EmptyLineEvent',
            'ProjectAlreadyExistsEvent',
            'SeparatorEvent',
        ])
        self.assertEqual(expected_events_already_exists, init_run_already_exists_results)
        
        # Set up wkdir for the next test case
        self._set_up_wkdir()

        
# EOF