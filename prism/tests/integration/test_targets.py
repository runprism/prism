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

class TestTargetIntegration(integration_test_class.IntegrationTestCase): 

    
    def test_targets(self):
        """
        `prism spark-submit` on project `008_targets` produces the expected targets
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '008_targets'
        os.chdir(wkdir)

        # Remove all compiled modules
        self._remove_compiled_dir(wkdir)

        # Remove all folders / files in the output directory
        self._remove_dirs_in_output(wkdir)
        self._remove_files_in_output(wkdir)

        # Execute command.
        args = ['spark-submit']
        spark_submit = self._run_prism(args)
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.json').is_file())
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.json'))
        for module in ['parquet.py', 'txt.py', 'csv.py', 'csv_mult.py']:
            refs = self._load_module_refs(module, manifest)
            self.assertEqual([], refs)

        # Check contents of output

        # Parquet target
        target_parquet_df = pd.read_parquet(Path(wkdir / 'output' / 'target_parquet'))
        expected_df = pd.DataFrame({
            'col1': ['col1_value1', 'col1_value2', 'col1_value3', 'col1_value4', 'col1_value5', 'col1_value6'],
            'col2': ['col2_value1', 'col2_value2', 'col2_value3', 'col2_value4', 'col2_value5', 'col2_value6'],
            'col3': ['col3_value1', 'col3_value2', 'col3_value3', 'col3_value4', 'col3_value5', 'col3_value6'],
        })
        self.assertTrue(target_parquet_df.equals(expected_df))

        # Txt target
        target_txt_txt = self._file_as_str(Path(wkdir / 'output' / 'target_txt.txt'))
        self.assertEqual('Hello, world!', target_txt_txt)

        # CSV (single target)
        target_csv_df = pd.read_csv(Path(wkdir / 'output' / 'target_csv.csv'), index_col=None)
        expected_df = pd.DataFrame({
            'col1': ['col1_value1', 'col1_value2', 'col1_value3'],
            'col2': ['col2_value1', 'col2_value2', 'col2_value3'],
            'col3': ['col3_value1', 'col3_value2', 'col3_value3'],
            'col4': ['col4_value1', 'col4_value2', 'col4_value3'],
            'col5': ['col5_value1', 'col5_value2', 'col5_value3'],
            'col6': ['col6_value1', 'col6_value2', 'col6_value3']
        })
        self.assertTrue(target_csv_df.equals(expected_df))

        # CSV (multiple targets)
        target_csv_mult_df1 = pd.read_csv(Path(wkdir / 'output' / 'target_csv_mult_df1.csv'), index_col=None)
        target_csv_mult_df2 = pd.read_csv(Path(wkdir / 'output' / 'target_csv_mult_df2.csv'), index_col=None)
        expected_df1 = pd.DataFrame({
            'col1': ['col1_value1', 'col1_value2', 'col1_value3'],
            'col2': ['col2_value1', 'col2_value2', 'col2_value3'],
            'col3': ['col3_value1', 'col3_value2', 'col3_value3'],
            'col4': ['col4_value1', 'col4_value2', 'col4_value3'],
            'col5': ['col5_value1', 'col5_value2', 'col5_value3'],
            'col6': ['col6_value1', 'col6_value2', 'col6_value3']
        })

        expected_df2 = pd.DataFrame({
            'colA': ['colA_value1', 'colA_value2', 'colA_value3'],
            'colB': ['colB_value1', 'colB_value2', 'colB_value3'],
            'colC': ['colC_value1', 'colC_value2', 'colC_value3'],
            'colD': ['colD_value1', 'colD_value2', 'colD_value3'],
            'colE': ['colE_value1', 'colE_value2', 'colE_value3'],
            'colF': ['colF_value1', 'colF_value2', 'colF_value3']
        })
        self.assertTrue(target_csv_mult_df1.equals(expected_df1))
        self.assertTrue(target_csv_mult_df2.equals(expected_df2))

        # CSV (iterated targets)
        target_csv_iter_df1 = pd.read_csv(Path(wkdir / 'output' / 'target_csv_iter_df1.csv'), index_col=None)
        target_csv_iter_df2 = pd.read_csv(Path(wkdir / 'output' / 'target_csv_iter_df2.csv'), index_col=None)
        self.assertTrue(target_csv_iter_df1.equals(expected_df1))
        self.assertTrue(target_csv_iter_df2.equals(expected_df2))

        # Remove all files in the output directory (to avoid having to commit and re-commit to Github)
        self._remove_parquet_files_in_dir(Path(wkdir / 'output' / 'target_parquet'))

        # Remove all compiled modules
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for next test
        self._set_up_wkdir()


# EOF