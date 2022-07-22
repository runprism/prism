"""
Base class for integration tests.

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
import io
import boto3
import ast
import astor
import os
from pathlib import Path
import unittest
import shutil
import yaml
import pandas as pd
import time
import subprocess

# Prism imports
import prism.cli.base
from prism.main import main
import prism.logging
from prism.constants import ROOT_DIR

# Ignore ResourceWarnings introduced by boto3
import warnings

###################################
## Test case directory and paths ##
###################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TEST_PROJECTS = Path(TEST_CASE_WKDIR) / 'test_projects'


################################
## Test case class definition ##
################################

class IntegrationTestCase(unittest.TestCase):


    def _set_up_wkdir(self):
        os.chdir(TEST_PROJECTS)


    def _is_valid_project(self, path):
        """
        Determine if `path` is a valid project (i.e., that is has a `prism_project.py` file and a `modules` folder)

        args:
            path: project path
        returns:
            boolean indicating whether `path` is a valid project
        """
        os.chdir(path)
        project_dir = prism.cli.base.get_project_dir()
        self.assertTrue(project_dir==path)
        self.assertTrue(Path(project_dir / 'modules').is_dir())
    

    def _load_manifest(self, path: Path) -> dict:
        """
        Load manifest
        """
        with open(path, 'r') as f:
            manifest = yaml.safe_load(f)
        f.close()
        return manifest
    

    def _run_prism(self, args: list):
        """
        Run prism using `args`
        """
        return main(args, bool_return=True)

        
    def _ignore_warnings(test_func):
        """
        Decorator to ignore ResourceWarnings during unittest functions. These arise due to some weird behavior by boto3.
        """
        def do_test(self, *args, **kwargs):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", ResourceWarning)
                return test_func(self, *args, **kwargs)
        return do_test


    def _get_profile_name(self, wkdir):
        """
        Get the profile name from the profile.yml file at `wkdir`
        """
        with open(Path(wkdir / 'profile.yml')) as f:
            yml_dict = yaml.safe_load(f)
        f.close()
        return list(yml_dict.keys())[0]

    
    def _delete_obj_from_s3(self, profile, bucket, path):
        """
        Delete an object from S3

        args:
            profile: name of profile for boto3 client
            bucket: bucket containing parquet file
            path: path to parquet file
        returns:
            None
        """
        session = boto3.session.Session(profile_name=profile)
        client = session.client('s3')
        s3 = session.resource('s3')
        objects_dict = client.list_objects_v2(Bucket=bucket, Prefix=path)
        s3_keys = [item['Key'] for item in objects_dict['Contents']]
        for key in s3_keys:
            client.delete_object(Bucket=bucket, Key=key)


    def _download_s3_file(self, s3, bucket, key):
        """
        Download parquet file from S3

        args:
            s3: boto3 S3 resource
            bucket: name of bucket containing parquet file
            key: key associated with parquet file
        returns:
            S3 object buffer for parquet file at `bucket` and `key`
        """
        buffer = io.BytesIO()
        s3.Object(bucket, key).download_fileobj(buffer)
        return buffer

    
    def _get_csv_file_in_s3_as_pd(self, profile, bucket, path, **kwargs):
        """
        Get parquet file in an S3 bucket as a pandas DataFrame

        args:
            profile: name of profile for boto3 client
            bucket: bucket containing parquet file
            path: path to parquet file
        returns:
            parquet file at `bucket`/`path` as a Pandas DataFrame
        """
        session = boto3.session.Session(profile_name=profile)
        client = session.client('s3')
        s3 = session.resource('s3')
        response = client.get_object(Bucket=bucket, Key=path)
        return pd.read_csv(response.get("Body"), **kwargs)


    def _remove_compiled_dir(self, wkdir):
        """
        Remove the .compiled directory, if it exists
        """
        if Path(wkdir / '.compiled').is_dir():
            shutil.rmtree(Path(wkdir / '.compiled'))
    

    def _remove_files_in_output(self, wkdir):
        """
        Remove file outputs from `output` folder of project
        """
        for file in Path(wkdir / 'output').iterdir():
            if Path(wkdir / 'output' / file).is_file():
                os.unlink(file)
    

    def _remove_dirs_in_output(self, wkdir):
        """
        Remove directory outputs from `output` folder of project
        """
        for file in Path(wkdir / 'output').iterdir():
            if Path(wkdir / 'output' / file).is_dir():
                shutil.rmtree(Path(wkdir / 'output' / file))

    def _remove_parquet_files_in_dir(self, dir):
        """
        Remove parquet files in directory
        """
        for filename in Path(dir).iterdir():
            if str(filename).split('.')[-1]=="parquet":
                os.unlink(filename)
            elif str(filename).split('.')[-1]=="crc" and str(filename).split('.')[-2]=="parquet":
                os.unlink(filename)


    def _file_as_str(self, path):
        """
        Open file as string
        """
        with open(path, 'r') as f:
            compiled_module_str = f.read()
        f.close()
        return compiled_module_str


    def _compiled_module_if_name_main(self, path):
        """
        Get `if __name__=="__main__"` body from `path
        """
        compiled_module_str = self._file_as_str(path)
        if_name_main_body = self._get_if_name_main_body(compiled_module_str)
        return if_name_main_body

    
    def _get_if_name_main_body(self, module_str: str) -> str:
        """
        Get the body of `if __name__ == "__main__"` and return it as a string

        args:
            module_str: module with `if __name__ == "__main__"` as a string
        returns:
            the body of `if __name__ == "__main__"`
        """
        module_ast_tree = ast.parse(module_str)
        self.assertTrue(isinstance(module_ast_tree, ast.Module))
        if_name_main_block = module_ast_tree.body[-1]
        self.assertTrue(isinstance(if_name_main_block, ast.If))
        return astor.to_source(if_name_main_block)


# EOF