"""
Integration tests.

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
import re
import shutil
import unittest
import pandas as pd
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


################################
## Test case class definition ##
################################

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

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Execute command.
        args = ['spark-submit']
        spark_submit = self._run_prism(args)
        self.assertFalse(Path(wkdir / 'output' / 'module01.txt').is_file())
        self.assertFalse(Path(wkdir / 'output' / 'module02.txt').is_file())

        # Execute run command to re-create module outputs
        args = ['run']
        self._run_prism(args)
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()


    def test_spark_project_all_modules(self):
        """
        `prism spark-submit` with all modules
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
        spark_submit_run = self._run_prism(args)

        # Check manifest
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.yml').is_file())
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.yml'))
        manifest_elems = manifest['manifest']
        for module in ['module01.py', 'module02.py', 'module03.py', 'module04.py']:
            self.assertEqual('success', manifest_elems[module]['status'])
        self.assertEqual([], manifest_elems['module01.py']['refs'])
        self.assertEqual('module01.py', manifest_elems['module02.py']['refs'])
        self.assertEqual('module02.py', manifest_elems['module03.py']['refs'])
        self.assertEqual('module03.py', manifest_elems['module04.py']['refs'])

        # Set up wkdir for the next test case
        self._set_up_wkdir()

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory (to avoid having to commit and re-commit to Github)
        self._remove_dirs_in_output(wkdir)
    

    def test_spark_project_subset(self):
        """
        `prism spark-submit` on a subset of modules at a time
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
        # Run only module 1 #
        # ***************** #

        # Expecatation: module 1 is the first module in the DAG. Therefore, we should not encounter any errors with this
        # command.
        args = ['spark-submit', '--modules', 'module01.py']
        spark_submit_run = self._run_prism(args)

        # Check manifest
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.yml').is_file())
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.yml'))
        manifest_elems = manifest['manifest']
        self.assertTrue('module01.py' in manifest_elems.keys())
        for module in ['module02.py', 'module03.py', 'module04.py']:
            self.assertFalse(module in manifest_elems.keys())
        self.assertEqual('success', manifest_elems['module01.py']['status'])
        self.assertEqual([], manifest_elems['module01.py']['refs'])

        # Check the results of the output directory
        self.assertTrue(Path(wkdir / 'output' / 'module01').is_dir())
        self.assertFalse(Path(wkdir / 'output' / 'module02').is_dir())
        module01_df = pd.read_parquet(Path(wkdir / 'output' / 'module01'))
        self.assertEqual(['col1', 'col2', 'col3'], list(module01_df.columns))
        self.assertEqual('col1_value1', module01_df['col1'][0])


        # **************** #
        # Execute module 2 #
        # **************** #
        
        # Expecatation: module 2 depends on module 1. However, since we just ran module 1, and the output of module 1 is
        # stored in a target, we do not need to re-run module 1 in order to run module 2. Therefore, we should not
        # encounter any errors with this command.
        args = ['spark-submit', '--modules', 'module02.py']
        spark_submit_run = self._run_prism(args)

        # Check the results of the output directory
        self.assertTrue(Path(wkdir / 'output' / 'module01').is_dir())
        self.assertTrue(Path(wkdir / 'output' / 'module02').is_dir())
        module02_df = pd.read_parquet(Path(wkdir / 'output' / 'module02'))
        self.assertEqual(['col1', 'col2', 'col3'], list(module02_df.columns))
        self.assertEqual(5, module02_df.shape[0])
        module02_df.sort_values(by='col1', inplace=True)
        module02_df.reset_index(inplace=True)
        module02_df.drop(columns=['index'], inplace=True)
        self.assertEqual('col1_value2', module02_df['col1'][0])


        # ************************************************* #
        # Execute module 4 (with and without `all-upstream` #
        # ************************************************* #

        # Expectation: module 4 depends on module 3. However, the output of module 3 is not stored in a target.
        # Therefore, running module 4 without including 'all-upstream' should cause an error.

        # Execute command without `all-upstream`
        # -------------------------------------
        args = ['spark-submit', '--modules', 'module04.py']
        spark_submit_run = self._run_prism(args)
        self.assertFalse(Path(wkdir / 'output' / 'module04').is_dir())


        # Execute command with `all-upstream`
        # -----------------------------------
        args = ['spark-submit', '--modules', 'module04.py', '--all-upstream']
        spark_submit_run = self._run_prism(args)
        self.assertTrue(Path(wkdir / 'output' / 'module04').is_dir())
        module04_df = pd.read_parquet(Path(wkdir / 'output' / 'module04'))
        self.assertEqual(['col1', 'col2', 'col3'], list(module04_df.columns))
        self.assertEqual(3, module04_df.shape[0])
        module04_df.sort_values(by='col1', inplace=True)
        module04_df.reset_index(inplace=True)
        module04_df.drop(columns=['index'], inplace=True)
        self.assertEqual('col1_value4', module04_df['col1'][0])

        # Remove parquet files from outputs (to avoid re-comitting to Github)
        self._remove_parquet_files_in_dir(Path(wkdir / 'output' / 'module01'))
        self._remove_parquet_files_in_dir(Path(wkdir / 'output' / 'module02'))
        self._remove_parquet_files_in_dir(Path(wkdir / 'output' / 'module04'))

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for next tst
        self._set_up_wkdir()



# EOF