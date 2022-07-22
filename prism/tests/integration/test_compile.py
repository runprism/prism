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
    'CompileStartEvent',
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
    'CompileStartEvent',
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
        self.assertFalse(Path(wkdir / '.compiled' / 'manifest.yml').is_file())

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
        self.assertFalse(Path(wkdir / '.compiled' / 'manifest.yml').is_file())
        
        # Set up wkdir for the next test case
        shutil.rmtree(Path(wkdir / '.compiled'))
        self._set_up_wkdir()
    

    def test_simple_project_one_modules_no_deps(self):
        """
        `prism compile` using one module with no dependencies
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '004_simple_project'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        if Path(wkdir / '.compiled').is_dir():
            shutil.rmtree(Path(wkdir / '.compiled'))

        # Execute command
        args = ['compile', '--modules', 'module03.py']
        compile_run = self._run_prism(args)
        compile_run_results = compile_run.get_results()
        self.assertEqual(' | '.join(simple_project_expected_events), compile_run_results)

        # Check that .compiled directory is formed
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.yml').is_file())

        # Check elements of manifest
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.yml'))
        manifest_elems = manifest['manifest']
        self.assertTrue('module03.py' in list(manifest_elems.keys()))
        self.assertFalse('module01.py' in list(manifest_elems.keys()))
        self.assertFalse('module02.py' in list(manifest_elems.keys()))
        self.assertEqual('success', manifest_elems['module03.py']['status'])
        self.assertEqual([], manifest_elems['module03.py']['refs'])

        # Set up wkdir for the next test case
        shutil.rmtree(Path(wkdir / '.compiled'))
        self._set_up_wkdir()
    

    def test_simple_project_one_modules_yes_deps(self):
        """
        `prism compile` using one module with dependencies
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '004_simple_project'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        if Path(wkdir / '.compiled').is_dir():
            shutil.rmtree(Path(wkdir / '.compiled'))
        
        # Execute command
        args = ['compile', '--modules', 'module02.py']
        compile_run = self._run_prism(args)
        compile_run_results = compile_run.get_results()
        self.assertEqual(' | '.join(simple_project_expected_events), compile_run_results)

        # Check that .compiled directory is formed and only expected modules are compiled
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.yml').is_file())

        # Check elements of manifest
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.yml'))
        manifest_elems = manifest['manifest']
        self.assertTrue('module01.py' in list(manifest_elems.keys()))
        self.assertTrue('module02.py' in list(manifest_elems.keys()))
        self.assertFalse('module03.py' in list(manifest_elems.keys()))

        for module in ['module01.py', 'module02.py']:
            self.assertEqual('success', manifest_elems[module]['status'])
        self.assertEqual([], manifest_elems['module01.py']['refs'])
        self.assertEqual('module01.py', manifest_elems['module02.py']['refs'])

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
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.yml').is_file())

        # Check elements of manifest
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
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.yml').is_file())

        # Check elements of manifest
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.yml'))
        manifest_elems = manifest['manifest']
        self.assertTrue('extract/module01.py' in list(manifest_elems.keys()))
        self.assertTrue('extract/module02.py' in list(manifest_elems.keys()))
        self.assertTrue('load/module03.py' in list(manifest_elems.keys()))
        self.assertTrue('module04.py' in list(manifest_elems.keys()))

        for module in ['extract/module01.py', 'extract/module02.py', 'load/module03.py', 'module04.py']:
            self.assertEqual('success', manifest_elems[module]['status'])
        
        # Set up wkdir for the next test case
        shutil.rmtree(Path(wkdir / '.compiled'))
        self._set_up_wkdir()
    

    def test_bad_mod_ref(self):
        """
        `prism compile` fails in a project with a bad mod ref
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '011_bad_mod_ref'
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
        self.assertFalse(Path(wkdir / '.compiled' / 'manifest.yml').is_file())
        
        # Set up wkdir for the next test case
        shutil.rmtree(Path(wkdir / '.compiled'))
        self._set_up_wkdir()

        
# EOF