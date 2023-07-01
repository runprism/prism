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

class TestSparkSubmitIntegration(integration_test_class.IntegrationTestCase):

    def test_project_no_profile(self):
        """
        `prism spark-submit` on a project without a valid profile throws an error
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '005_simple_project_no_null'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Execute command.
        args = ['spark-submit']
        self._run_prism(args)
        self.assertFalse(Path(wkdir / 'output' / 'model01.txt').is_file())
        self.assertFalse(Path(wkdir / 'output' / 'model02.txt').is_file())

        # Execute run command to re-create model outputs
        args = ['run']
        self._run_prism(args)
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_spark_project_all_models(self):
        """
        `prism spark-submit` with all models
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '007_spark_project'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_dirs_in_output(wkdir)

        # Execute spark-submit command
        args = ['spark-submit']
        self._run_prism(args)

        # Check manifest
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.json').is_file())
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.json'))
        model01_refs = self._load_model_refs("model01.py", manifest)
        model02_refs = self._load_model_refs("model02.py", manifest)
        model03_refs = self._load_model_refs("model03.py", manifest)
        model04_refs = self._load_model_refs("model04.py", manifest)

        self.assertEqual([], model01_refs)
        self.assertEqual('model01.py', model02_refs)
        self.assertEqual('model02.py', model03_refs)
        self.assertEqual('model03.py', model04_refs)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory (to avoid having to commit and
        # re-commit to Github)
        self._remove_dirs_in_output(wkdir)

    def test_spark_project_subset(self):
        """
        `prism spark-submit` on a subset of models at a time
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '007_spark_project'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_dirs_in_output(wkdir)

        # ***************** #
        # Run only model 1 #
        # ***************** #

        # Expecatation: model 1 is the first model in the DAG. Therefore, we should
        # not encounter any errors with this command.
        args = ['spark-submit', '--model', 'model01.py']
        self._run_prism(args)

        # Check manifest
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.json').is_file())
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.json'))
        model01_refs = self._load_model_refs("model01.py", manifest)
        model02_refs = self._load_model_refs("model02.py", manifest)
        model03_refs = self._load_model_refs("model03.py", manifest)
        model04_refs = self._load_model_refs("model04.py", manifest)

        self.assertEqual([], model01_refs)
        self.assertEqual('model01.py', model02_refs)
        self.assertEqual('model02.py', model03_refs)
        self.assertEqual('model03.py', model04_refs)

        # Check the results of the output directory
        self.assertTrue(Path(wkdir / 'output' / 'model01').is_dir())
        self.assertFalse(Path(wkdir / 'output' / 'model02').is_dir())
        model01_df = pd.read_parquet(Path(wkdir / 'output' / 'model01'))
        self.assertEqual(['col1', 'col2', 'col3'], list(model01_df.columns))
        self.assertEqual('col1_value1', model01_df['col1'][0])

        # **************** #
        # Execute model 2 #
        # **************** #

        # Expecatation: model 2 depends on model 1. However, since we just ran model
        # 1, and the output of model 1 is stored in a target, we do not need to re-run
        # model 1 in order to run model 2. Therefore, we should not encounter any
        # errors with this command.
        args = ['spark-submit', '--model', 'model02.py']
        self._run_prism(args)

        # Check the results of the output directory
        self.assertTrue(Path(wkdir / 'output' / 'model01').is_dir())
        self.assertTrue(Path(wkdir / 'output' / 'model02').is_dir())
        model02_df = pd.read_parquet(Path(wkdir / 'output' / 'model02'))
        self.assertEqual(['col1', 'col2', 'col3'], list(model02_df.columns))
        self.assertEqual(5, model02_df.shape[0])
        model02_df.sort_values(by='col1', inplace=True)
        model02_df.reset_index(inplace=True)
        model02_df.drop(columns=['index'], inplace=True)
        self.assertEqual('col1_value2', model02_df['col1'][0])

        # ************************************************* #
        # Execute model 4 (with and without `all-upstream` #
        # ************************************************* #

        # Expectation: model 4 depends on model 3. However, the output of model 3 is
        # not stored in a target. Therefore, running model 4 without including
        # 'all-upstream' should cause an error.

        # -------------------------------------
        # Execute command without `all-upstream`
        args = ['spark-submit', '--model', 'model04.py']
        self._run_prism(args)
        self.assertFalse(Path(wkdir / 'output' / 'model04').is_dir())

        # -----------------------------------
        # Execute command with `all-upstream`
        args = ['spark-submit', '--model', 'model04.py', '--all-upstream']
        self._run_prism(args)
        self.assertTrue(Path(wkdir / 'output' / 'model04').is_dir())
        model04_df = pd.read_parquet(Path(wkdir / 'output' / 'model04'))
        self.assertEqual(['col1', 'col2', 'col3'], list(model04_df.columns))
        self.assertEqual(3, model04_df.shape[0])
        model04_df.sort_values(by='col1', inplace=True)
        model04_df.reset_index(inplace=True)
        model04_df.drop(columns=['index'], inplace=True)
        self.assertEqual('col1_value4', model04_df['col1'][0])

        # Remove parquet files from outputs (to avoid re-comitting to Github)
        self._remove_parquet_files_in_dir(Path(wkdir / 'output' / 'model01'))
        self._remove_parquet_files_in_dir(Path(wkdir / 'output' / 'model02'))
        self._remove_parquet_files_in_dir(Path(wkdir / 'output' / 'model04'))

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for next tst
        self._set_up_wkdir()

    def test_all_downstream(self):
        """
        `prism spark-submit` with `all-downstream` works as expected
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '007_spark_project'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_dirs_in_output(wkdir)

        # Run all models downstream of model01.py
        args = ['spark-submit', '--model', 'model01.py', '--all-downstream']
        self._run_prism(args)

        # Check manifest
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.json').is_file())
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.json'))
        model01_refs = self._load_model_refs("model01.py", manifest)
        model02_refs = self._load_model_refs("model02.py", manifest)
        model03_refs = self._load_model_refs("model03.py", manifest)
        model04_refs = self._load_model_refs("model04.py", manifest)

        self.assertEqual([], model01_refs)
        self.assertEqual('model01.py', model02_refs)
        self.assertEqual('model02.py', model03_refs)
        self.assertEqual('model03.py', model04_refs)

        # ------------------------------------------------------------------------------
        # Check the results of the output directory

        # ------------------
        # Output files exist
        self.assertTrue(Path(wkdir / 'output' / 'model01').is_dir())
        self.assertTrue(Path(wkdir / 'output' / 'model02').is_dir())
        self.assertTrue(Path(wkdir / 'output' / 'model04').is_dir())

        # Output structure are what we expect
        model01_df = pd.read_parquet(Path(wkdir / 'output' / 'model01'))
        model02_df = pd.read_parquet(Path(wkdir / 'output' / 'model02'))
        model04_df = pd.read_parquet(Path(wkdir / 'output' / 'model04'))
        self.assertEqual(['col1', 'col2', 'col3'], list(model01_df.columns))
        self.assertEqual(['col1', 'col2', 'col3'], list(model02_df.columns))
        self.assertEqual(['col1', 'col2', 'col3'], list(model04_df.columns))

        # ----------------------------------
        # Output contents are what we expect

        # Model 1
        self.assertEqual('col1_value1', model01_df['col1'][0])
        self.assertEqual(5, model02_df.shape[0])

        # Model 2
        model02_df.sort_values(by='col1', inplace=True)
        model02_df.reset_index(inplace=True)
        model02_df.drop(columns=['index'], inplace=True)
        self.assertEqual('col1_value2', model02_df['col1'][0])

        # Model 4
        self.assertEqual(3, model04_df.shape[0])
        model04_df.sort_values(by='col1', inplace=True)
        model04_df.reset_index(inplace=True)
        model04_df.drop(columns=['index'], inplace=True)
        self.assertEqual('col1_value4', model04_df['col1'][0])

        # ----------------------------------
        # Cleanup

        # Remove parquet files from outputs (to avoid re-comitting to Github)
        self._remove_parquet_files_in_dir(Path(wkdir / 'output' / 'model01'))
        self._remove_parquet_files_in_dir(Path(wkdir / 'output' / 'model02'))
        self._remove_parquet_files_in_dir(Path(wkdir / 'output' / 'model04'))

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for next test
        self._set_up_wkdir()
