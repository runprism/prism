"""
Integration tests.

Table of Contents:
- Imports
- Test case directory and paths
- Expected events
- Test case class definition
"""

#############
## Imports ##
#############

# Standard library imports
import io
import boto3
import pandas as pd
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

# Ignore ResourceWarnings produced by boto3 during unittests
import warnings


###################################
## Test case directory and paths ##
###################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TEST_PROJECTS = Path(TEST_CASE_WKDIR) / 'test_projects'


#####################
## Expected events ##
#####################

def _execution_events_modules(module_names_statuses: dict) -> list:
    """
    Create list for execution events

    args:
        module_names_statuses: dict for execution events, where keys are the names of the events and values are the 
          eventual statuses
    returns:
        list of execution events
    """
    results = []
    for k,v in module_names_statuses.items():
        results.append(f'ExecutionEvent - {k} - RUN')
        results.append(f'ExecutionEvent - {k} - {v}')
    return results


def _run_task_end_events(end_event_name: str) -> list:
    """
    Create list for events marking the end of the run task

    args:
        end_event_name: name of end event
    returns:
        list of events marking end of run task
    """
    return [
        'EmptyLineEvent',
        end_event_name,
        'SeparatorEvent'
    ]


# Starting events for a successful run task. We will only run tasks that successfully compile, since we have dealt with
# compile edge cases in the `test_compile` integration tests.
run_success_starting_events = [
    'SeparatorEvent',
    'TaskRunEvent',
    'CurrentProjectDirEvent',
    'CompileStartEvent',
    'EmptyLineEvent',
    'ExecutionEvent - module DAG - RUN',
    'ExecutionEvent - module DAG - DONE',
    'ExecutionEvent - parsing config files - RUN',
    'ExecutionEvent - parsing config files - DONE',
    'ExecutionEvent - creating pipeline, DAG executor - RUN',
    'ExecutionEvent - creating pipeline, DAG executor - DONE',
    'EmptyLineEvent'
]


simple_project_all_modules_expected_events = run_success_starting_events + \
    _execution_events_modules({'module03.py': 'ERROR'}) + \
    _run_task_end_events('PrismExceptionErrorEvent')

simple_project_no_null_all_modules_expected_events = run_success_starting_events + \
    _execution_events_modules({
        'module01.py': 'DONE',
        'module02.py': 'DONE',
        'module03.py': 'DONE',
        'module04.py': 'DONE',
    }) + _run_task_end_events('TaskSuccessfulEndEvent')


################################
## Test case class definition ##
################################

class TestRunIntegration(integration_test_class.IntegrationTestCase):


    def test_simple_project_all_modules(self):
        """
        `prism run` on simple project with a null task output
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '004_simple_project'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Execute command
        args = ['run']
        runtask_run = self._run_prism(args)
        runtask_run_results = runtask_run.get_results()
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.yml').is_file())

        # Check events
        self.assertEqual(' | '.join(simple_project_all_modules_expected_events), runtask_run_results)

        # Check manifest.yml
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.yml'))
        manifest_elems = manifest['manifest']
        self.assertTrue('module01.py' in list(manifest_elems.keys()))
        self.assertTrue('module02.py' in list(manifest_elems.keys()))
        self.assertTrue('module03.py' in list(manifest_elems.keys()))
        for module in ['module01.py', 'module02.py', 'module03.py']:
            self.assertEqual('success', manifest_elems[module]['status'])
        self.assertEqual([], manifest_elems['module01.py']['refs'])
        self.assertEqual('module01.py', manifest_elems['module02.py']['refs'])
        self.assertEqual([], manifest_elems['module03.py']['refs'])

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()


    def test_simple_project_no_null_all_modules(self):
        """
        `prism run` on simple project with no null task outputs
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '005_simple_project_no_null'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Execute command
        args = ['run']
        runtask_run = self._run_prism(args)
        runtask_run_results = runtask_run.get_results()
        self.assertEqual(' | '.join(simple_project_no_null_all_modules_expected_events), runtask_run_results)
        self.assertTrue(Path(wkdir / 'output' / 'module01.txt').is_file())
        self.assertTrue(Path(wkdir / 'output' / 'module02.txt').is_file())
        
        # Check contents
        module01_txt = self._file_as_str(Path(wkdir / 'output' / 'module01.txt'))
        module02_txt = self._file_as_str(Path(wkdir / 'output' / 'module02.txt'))
        self.assertEqual('Hello from module 1!', module01_txt)
        self.assertEqual('Hello from module 1!' + '\n' + 'Hello from module 2!', module02_txt)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    
    def test_simple_project_no_null_subset(self):
        """
        `prism run` on simple project with no null task outputs
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '005_simple_project_no_null'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # ***************** #
        # Run only module 1 #
        # ***************** #

        # Expecatation: module 1 is the first module in the DAG. Therefore, we should not encounter any errors with this
        # command.
        args = ['run', '--modules', 'module01.py']
        runtask_run = self._run_prism(args)
        runtask_run_results = runtask_run.get_results()
        expected_events = run_success_starting_events + \
            _execution_events_modules({'module01.py': 'DONE'}) + \
            _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), runtask_run_results)

        # Check the results of the output directory
        self.assertTrue(Path(wkdir / 'output' / 'module01.txt').is_file())
        self.assertFalse(Path(wkdir / 'output' / 'module02.txt').is_file())
        module01_txt = self._file_as_str(Path(wkdir / 'output' / 'module01.txt'))
        self.assertEqual('Hello from module 1!', module01_txt)

        # Check manifest
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.yml').is_file())
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.yml'))
        manifest_elems = manifest['manifest']
        self.assertTrue('module01.py' in list(manifest_elems.keys()))
        self.assertFalse('module02.py' in list(manifest_elems.keys()))


        # **************** #
        # Execute module 2 #
        # **************** #

        # Expecatation: module 2 depends on module 1. However, since we just ran module 1, and the output of module 1 is
        # stored in a target, we do not need to re-run module 1 in order to run module 2. Therefore, we should not
        # encounter any errors with this command.

        # Execute command
        args = ['run', '--modules', 'module02.py']
        runtask_run = self._run_prism(args)
        runtask_run_results = runtask_run.get_results()
        expected_events = run_success_starting_events + \
            _execution_events_modules({'module02.py': 'DONE'}) + \
            _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), runtask_run_results)

        # Check the results of the output directory
        self.assertTrue(Path(wkdir / 'output' / 'module02.txt').is_file())
        with open(Path(wkdir / 'output' / 'module02.txt'), 'r') as f:
            module02_txt = f.read()
        f.close()
        self.assertEqual('Hello from module 1!' + '\n' + 'Hello from module 2!', module02_txt)


        # ************************************************* #
        # Execute module 4 (with and without `all-upstream` #
        # ************************************************* #

        # Expectation: module 4 depends on module 3. However, the output of module 3 is not stored in a target.
        # Therefore, running module 4 without including 'all-upstream' should cause an error.

        # Execute command without `all-upstream`
        # -------------------------------------
        args = ['run', '--modules', 'module04.py']
        runtask_run = self._run_prism(args)
        runtask_run_results = runtask_run.get_results()
        expected_events = run_success_starting_events + \
            _execution_events_modules({'module04.py': 'ERROR'}) + \
            _run_task_end_events('PrismExceptionErrorEvent')
        self.assertEqual(' | '.join(expected_events), runtask_run_results)


        # Execute command with `all-upstream`
        # -----------------------------------
        self._remove_compiled_dir(wkdir)
        self._remove_files_in_output(wkdir)
        args = ['run', '--modules', 'module04.py', '--all-upstream']
        runtask_run = self._run_prism(args)
        runtask_run_results = runtask_run.get_results()
        self.assertEqual(' | '.join(simple_project_no_null_all_modules_expected_events), runtask_run_results)

        # Check the results of the output directory
        self.assertTrue(Path(wkdir / 'output' / 'module01.txt').is_file())
        self.assertTrue(Path(wkdir / 'output' / 'module02.txt').is_file())
        module02_txt = self._file_as_str(Path(wkdir / 'output' / 'module02.txt'))
        self.assertEqual('Hello from module 1!' + '\n' + 'Hello from module 2!', module02_txt)

        # Check manifest
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.yml').is_file())
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.yml'))
        manifest_elems = manifest['manifest']
        self.assertTrue('module01.py' in list(manifest_elems.keys()))
        self.assertTrue('module02.py' in list(manifest_elems.keys()))
        self.assertTrue('module03.py' in list(manifest_elems.keys()))
        self.assertTrue('module04.py' in list(manifest_elems.keys()))

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()


    def test_project_nested_module_dirs(self):
        """
        `prism run` in a project with directories in the modules folder
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '010_project_nested_module_dirs'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Helper function
        def check_modules_1_2_results():
            """
            Helper function to check the results of running modules 1 and 2. We will use this a couple of times.
            """
            # Check that .compiled directory is formed
            self.assertTrue(Path(wkdir / '.compiled').is_dir())
            self.assertTrue(Path(wkdir / '.compiled' / 'manifest.yml').is_file())

            # Check manifest
            manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.yml'))
            manifest_elems = manifest['manifest']
            self.assertTrue('extract/module01.py' in manifest_elems.keys())
            self.assertTrue('extract/module02.py' in manifest_elems.keys())
            for module in ['extract/module01.py', 'extract/module02.py']:
                self.assertEqual('success', manifest_elems[module]['status'])
            self.assertEqual([], manifest_elems['extract/module01.py']['refs'])
            self.assertEqual('extract/module01.py', manifest_elems['extract/module02.py']['refs'])

            # Check that outputs are created
            self.assertTrue(Path(wkdir / 'output' / 'module01.txt').is_file())
            self.assertTrue(Path(wkdir / 'output' / 'module02.txt').is_file())
            with open(Path(wkdir / 'output' / 'module02.txt'), 'r') as f:
                module02_txt = f.read()
            f.close()
            self.assertEqual('Hello from module 1!' + '\n' + 'Hello from module 2!', module02_txt)


        # ****************************************************** #
        # Execute all modules in extract folder using '*' syntax #
        # ****************************************************** #

        args = ['run', '--modules', 'extract/*']
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = run_success_starting_events + \
            _execution_events_modules(
                {
                    'extract/module01.py': 'DONE',
                    'extract/module02.py': 'DONE'
                }) + \
            _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), run_results)

        # Check results
        check_modules_1_2_results()

        # Remove all files in the compiled and output directory
        self._remove_compiled_dir(wkdir)
        self._remove_files_in_output(wkdir)


        # ***************************************************************** #
        # Execute all modules in extract /load folder using explicit syntax #
        # ***************************************************************** #

        args = ['run', '--modules', 'extract/module01.py', 'extract/module02.py', 'load/module03.py']
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = run_success_starting_events + \
            _execution_events_modules(
                {
                    'extract/module01.py': 'DONE',
                    'extract/module02.py': 'DONE',
                    'load/module03.py': 'DONE',
                }) + \
            _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), run_results)

        # Check results
        check_modules_1_2_results()

        # Check manifest for load/module03
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.yml'))
        manifest_elems = manifest['manifest']
        self.assertTrue('load/module03.py' in manifest_elems.keys())
        self.assertEqual('success', manifest_elems['load/module03.py']['status'])
        self.assertEqual('extract/module02.py', manifest_elems['load/module03.py']['refs'])

        # Remove all files in the compiled and output directory
        self._remove_compiled_dir(wkdir)
        self._remove_files_in_output(wkdir)


        # ******************* #
        # Execute all modules #
        # ******************* #

        args = ['run']
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = run_success_starting_events + \
            _execution_events_modules(
                {
                    'extract/module01.py': 'DONE',
                    'extract/module02.py': 'DONE',
                    'load/module03.py': 'DONE',
                    'module04.py': 'DONE'
                }) + \
            _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), run_results)

        # Check output of modules 1 and 2
        check_modules_1_2_results()

        # Check manifest for load/module03
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.yml'))
        manifest_elems = manifest['manifest']
        for module in ['load/module03.py', 'module04.py']:
            self.assertTrue(module in manifest_elems.keys())
            self.assertEqual('success', manifest_elems[module]['status'])
        self.assertEqual('extract/module02.py', manifest_elems['load/module03.py']['refs'])
        self.assertEqual('load/module03.py', manifest_elems['module04.py']['refs'])

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()
    

    def test_bad_mod_ref(self):
        """
        `prism run` fails in a project with a bad mod ref
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '011_bad_mod_ref'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        if Path(wkdir / '.compiled').is_dir():
            shutil.rmtree(Path(wkdir / '.compiled'))

        args = ['run']
        run_run = self._run_prism(args)
        run_run_results = run_run.get_results()
        expected_events = [
            'SeparatorEvent',
            'TaskRunEvent',
            'CurrentProjectDirEvent',
            'CompileStartEvent',
            'EmptyLineEvent',
            'ExecutionEvent - module DAG - RUN',
            'ExecutionEvent - module DAG - ERROR',
        ] + _run_task_end_events('PrismExceptionErrorEvent')
        self.assertEqual(' | '.join(expected_events), run_run_results)

        # The .compiled directory will be created
        self.assertTrue(Path(wkdir / '.compiled').is_dir())

        # But, the manifest file will not be
        self.assertFalse(Path(wkdir / '.compiled' / 'manifest.yml').is_file())
        
        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()


# EOF