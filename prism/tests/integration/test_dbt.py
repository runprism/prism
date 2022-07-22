"""
Integration tests for dbt.

Table of Contents:
- Imports
- Test case directory and paths
- Test case class definition
"""

#############
## Imports ##
#############

# Standard library imports
import io
import boto3
import boto3.session
import os
from pathlib import Path
import pandas as pd
import shutil

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

class TestSparkSubmitIntegration(integration_test_class.IntegrationTestCase): 

    
    def test_simple_dbt_project(self):
        """
        `prism run` on project `009_simple_dbt_project` produces the expected targets
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '009_simple_dbt_project' / 'prism'
        os.chdir(wkdir)

        # Remove all compiled modules
        self._remove_compiled_dir(wkdir)

        # Remove all folders / files in the output directory
        self._remove_dirs_in_output(wkdir)
        self._remove_files_in_output(wkdir)

        # Execute command.
        args = ['run']
        run_results = self._run_prism(args)
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.yml').is_file())
        
        # Check contents of output
        df = pd.read_csv(wkdir / 'output' / 'jaffle_shop_customers.csv')
        expected_columns = [
            'CUSTOMER_ID',
            'FIRST_NAME',
            'LAST_NAME',
            'FIRST_ORDER',
            'MOST_RECENT_ORDER',
            'NUMBER_OF_ORDERS',
            'CUSTOMER_LIFETIME_VALUE'
        ]
        self.assertEqual(expected_columns, list(df.columns))
        id_1_first_name = df.loc[df['CUSTOMER_ID']==1, 'FIRST_NAME'][0]
        self.assertEqual('Michael', id_1_first_name)

        # Remove the 'target' -- it contains dbt artifacts
        if Path(wkdir / 'target').is_dir():
            shutil.rmtree(Path(wkdir / 'target'))

        # Remove compiled folder
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for next test case
        self._set_up_wkdir()


# EOF