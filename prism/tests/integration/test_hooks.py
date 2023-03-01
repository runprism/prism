"""
Integration tests.

Table of Contents:
- Imports
- Test case directory and paths
- Test case class definition
"""

###########
# Imports #
###########

# Standard library imports
import os
from pathlib import Path
import shutil
import pandas as pd

# Prism imports
import prism.tests.integration.integration_test_class as integration_test_class


#################################
# Test case directory and paths #
#################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TEST_PROJECTS = Path(TEST_CASE_WKDIR) / 'test_projects'


##############################
# Test case class definition #
##############################

class TestSqlIntegration(integration_test_class.IntegrationTestCase):

    def test_snowflake_spark(self):
        """
        Snowflake and PySpark hooks work as expected
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '013_hooks_sql_spark'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        if Path(wkdir / '.compiled').is_dir():
            shutil.rmtree(Path(wkdir / '.compiled'))
        self.maxDiff = None

        # ------------------------------------------------------------------------------
        # Run snowflake.py and spark.py

        args = ['spark-submit', '--modules', 'snowflake.py', 'spark.py']
        self._run_prism(args)

        # Get module 1 and 2 outputs
        machinery_sample = pd.read_csv(wkdir / 'output' / 'machinery_sample.csv')
        household_sample = pd.read_csv(wkdir / 'output' / 'household_sample.csv')
        machinery_sample_filtered = pd.read_csv(
            wkdir / 'output' / 'machinery_sample_filtered.csv'
        )
        household_sample_filtered = pd.read_csv(
            wkdir / 'output' / 'household_sample_filtered.csv'
        )

        # Sample data 1 and 2 should have 50 rows
        for df in [machinery_sample, household_sample]:
            self.assertEqual(df.shape[0], 50)

        # Sample data 1 should only have C_MKTSEGMENT = MACHINERY
        machinery_sample_mktsegment = list(machinery_sample['C_MKTSEGMENT'].unique())
        self.assertEqual(len(machinery_sample_mktsegment), 1)
        self.assertEqual(machinery_sample_mktsegment[0], 'MACHINERY')

        # Sample data 1 should only have C_MKTSEGMENT = HOUSEHOLD
        household_sample_mktsegment = list(household_sample['C_MKTSEGMENT'].unique())
        self.assertEqual(len(household_sample_mktsegment), 1)
        self.assertEqual(household_sample_mktsegment[0], 'HOUSEHOLD')

        # Filtered sample data 1 should only have C_ACCTBAL > 1000
        machinery_sample_min_acctbal = max(machinery_sample_filtered['C_ACCTBAL'])
        self.assertTrue(machinery_sample_min_acctbal <= 100000)

        # Filtered sample data 2 should only have C_ACCTBAL > 2000
        household_sample_min_acctbal = min(household_sample_filtered['C_ACCTBAL'])
        household_sample_max_acctbal = max(household_sample_filtered['C_ACCTBAL'])
        self.assertTrue(household_sample_min_acctbal > 100000)
        self.assertTrue(household_sample_max_acctbal <= 200000)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove stuff in output to avoid recommitting to github
        self._remove_files_in_output(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_bad_adapter(self):
        """
        Bad adapter will produce an error
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '013_hooks_sql_spark'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        if Path(wkdir / '.compiled').is_dir():
            shutil.rmtree(Path(wkdir / '.compiled'))
        self.maxDiff = None

        # ------------------------------------------------------------------------------
        # Run bad_adapter.py

        args = ['spark-submit', '--modules', 'bad_adapter.py']
        _ = self._run_prism(args)

        # We can't check the error events directly; for now, let's just check that the
        # output wasn't created.
        self.assertFalse(Path(wkdir / 'output' / 'bad_adapter.csv').is_file())

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove stuff in output to avoid recommitting to github
        self._remove_files_in_output(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_postgres(self):
        """
        Postgres hooks work as expected
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '013_hooks_sql_spark'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        if Path(wkdir / '.compiled').is_dir():
            shutil.rmtree(Path(wkdir / '.compiled'))
        self.maxDiff = None

        # Check that expected output doesn't already exist
        expected_output = Path(wkdir) / 'output' / 'sample_postgres_data.csv'
        self.assertFalse(expected_output.is_file())

        # Run project
        args = ['spark-submit', '--modules', 'postgres.py']
        self._run_prism(args)

        # Check output
        self.assertTrue(expected_output.is_file())
        df = pd.read_csv(expected_output)
        self.assertEqual(df.shape[0], 1)
        self.assertEqual(df.test_col[0], 1)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove stuff in output to avoid recommitting to github
        self._remove_files_in_output(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()
