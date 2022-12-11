"""
Integration tests.

Table of Contents:
- Imports
- Test case directory and paths
- Expected events
- Test case class definition
"""

###########
# Imports #
###########

# Standard library imports
import os
from pathlib import Path
import shutil
import unittest

# Prism imports
import prism.logging
import prism.tests.integration.integration_test_class as integration_test_class


#################################
# Test case directory and paths #
#################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TEST_PROJECTS = Path(TEST_CASE_WKDIR) / 'test_projects'


#####################
## Expected events ##
#####################

# Expected events for `compile` task when called on project with prism_project.py file
no_project_py_expected_events = [
    'SeparatorEvent',
    'TaskRunEvent',
    'EmptyLineEvent',
    'ProjectPyNotFoundEvent',
    'SeparatorEvent'
]

# Expected events for `compile` task when called on project with cycle
project_with_error_expected_events = [
    'SeparatorEvent',
    'TaskRunEvent',
    'CurrentProjectDirEvent',
    'EmptyLineEvent',
    'ExecutionEvent - module DAG - RUN',
    'ExecutionEvent - module DAG - ERROR',
    'EmptyLineEvent',
    'PrismExceptionErrorEvent',
    'SeparatorEvent'
]

# Expected events for `compile` task when called on a simple project. Not that the `compile` task does not fire events
# for each module that is compiled, so the different compile  CLI arguments below should produce the same events.
simple_project_expected_events = [
    'SeparatorEvent',
    'TaskRunEvent',
    'CurrentProjectDirEvent',
    'EmptyLineEvent',
    'ExecutionEvent - module DAG - RUN',
    'ExecutionEvent - module DAG - DONE',
    'EmptyLineEvent',
    'TaskSuccessfulEndEvent',
    'SeparatorEvent'
]


################################
## Test case class definition ##
################################

class TestCompileIntegration(integration_test_class.IntegrationTestCase):

    
    def test_no_project_py(self):
        """
        `prism compile` in a project without a `project.py` file
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '002_no_project_py'
        os.chdir(wkdir)

        # Execute command
        args = ['compile']
        compile_run = self._run_prism(args)
        compile_run_results = compile_run.get_results()
        self.assertEqual(' | '.join(no_project_py_expected_events), compile_run_results)

        # Check that none of the modules are compiled
        self.assertFalse(Path(wkdir / '.compiled').is_dir())
        self.assertFalse(Path(wkdir / '.compiled' / 'manifest.json').is_file())

        # Set up wkdir for the next test case
        self._set_up_wkdir()
    

    def test_project_with_cycle(self):
        """
        `prism compile` in a project with a cycle
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '003_project_with_cycle'
        os.chdir(wkdir)

        # Remove compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Execute command
        args = ['compile']
        compile_run = self._run_prism(args)
        compile_run_results = compile_run.get_results()
        self.assertEqual(' | '.join(project_with_error_expected_events), compile_run_results)

        # Check that none of the modules are compiled
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertFalse(Path(wkdir / '.compiled' / 'manifest.json').is_file())
        
        # Set up wkdir for the next test case
        shutil.rmtree(Path(wkdir / '.compiled'))
        self._set_up_wkdir()


    def test_simple_project_all_modules(self):
        """
        `prism compile` using all modules
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '004_simple_project'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        if Path(wkdir / '.compiled').is_dir():
            shutil.rmtree(Path(wkdir / '.compiled'))

        args = ['compile']
        compile_run = self._run_prism(args)
        compile_run_results = compile_run.get_results()
        self.assertEqual(' | '.join(simple_project_expected_events), compile_run_results)

        # Check that .compiled directory is formed
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.json').is_file())

        # Check elements of manifest
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.json'))
        module01_refs = self._load_module_refs("module01.py", manifest)
        module02_refs = self._load_module_refs("module02.py", manifest)
        module03_refs = self._load_module_refs("module03.py", manifest)
        self.assertEqual([], module01_refs)
        self.assertEqual('module01.py', module02_refs)
        self.assertEqual([], module03_refs)

        # Set up wkdir for the next test case
        shutil.rmtree(Path(wkdir / '.compiled'))
        self._set_up_wkdir()
    

    def test_project_nested_module_dirs(self):
        """
        `prism compile` in a project with directories in the modules folder
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '010_project_nested_module_dirs'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        if Path(wkdir / '.compiled').is_dir():
            shutil.rmtree(Path(wkdir / '.compiled'))

        args = ['compile']
        compile_run = self._run_prism(args)
        compile_run_results = compile_run.get_results()
        self.assertEqual(' | '.join(simple_project_expected_events), compile_run_results)

        # Check that .compiled directory is formed
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.json').is_file())

        # Check elements of manifest
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.json'))
        extract_module01_refs = self._load_module_refs("extract/module01.py", manifest)
        extract_module02_refs = self._load_module_refs("extract/module02.py", manifest)
        load_module03_refs = self._load_module_refs("load/module03.py", manifest)
        module04_refs = self._load_module_refs("module04.py", manifest)
        self.assertEqual([], extract_module01_refs)
        self.assertEqual("extract/module01.py", extract_module02_refs)
        self.assertEqual("extract/module02.py", load_module03_refs)
        self.assertEqual("load/module03.py", module04_refs)
        
        # Set up wkdir for the next test case
        shutil.rmtree(Path(wkdir / '.compiled'))
        self._set_up_wkdir()
    

    def test_bad_task_ref(self):
        """
        `prism compile` fails in a project with a bad mod ref
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '011_bad_task_ref'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        if Path(wkdir / '.compiled').is_dir():
            shutil.rmtree(Path(wkdir / '.compiled'))

        args = ['compile']
        compile_run = self._run_prism(args)
        compile_run_results = compile_run.get_results()
        self.assertEqual(' | '.join(project_with_error_expected_events), compile_run_results)

        # Check that .compiled directory is not created
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertFalse(Path(wkdir / '.compiled' / 'manifest.json').is_file())
        
        # Set up wkdir for the next test case
        shutil.rmtree(Path(wkdir / '.compiled'))
        self._set_up_wkdir()

        
# EOF