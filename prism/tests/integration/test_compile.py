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

# Prism imports
import prism.tests.integration.integration_test_class as integration_test_class


#################################
# Test case directory and paths #
#################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TEST_PROJECTS = Path(TEST_CASE_WKDIR) / 'test_projects'


###################
# Expected events #
###################

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
    'ExecutionEvent - task DAG - RUN',
    'ExecutionEvent - task DAG - ERROR',
    'EmptyLineEvent',
    'PrismExceptionErrorEvent',
    'SeparatorEvent'
]

# Expected events for `compile` task when called on a simple project. Not that the
# `compile` task does not fire events for each task that is compiled, so the different
# compile  CLI arguments below should produce the same events.
simple_project_expected_events = [
    'SeparatorEvent',
    'TaskRunEvent',
    'CurrentProjectDirEvent',
    'EmptyLineEvent',
    'ExecutionEvent - task DAG - RUN',
    'ExecutionEvent - task DAG - DONE',
    'EmptyLineEvent',
    'TaskSuccessfulEndEvent',
    'SeparatorEvent'
]


##############################
# Test case class definition #
##############################

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

        # Check that none of the tasks are compiled
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
        self.assertEqual(
            ' | '.join(project_with_error_expected_events),
            compile_run_results
        )

        # Check that none of the tasks are compiled
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertFalse(Path(wkdir / '.compiled' / 'manifest.json').is_file())

        # Set up wkdir for the next test case
        shutil.rmtree(Path(wkdir / '.compiled'))
        self._set_up_wkdir()

    def test_simple_project_all_tasks(self):
        """
        `prism compile` using all tasks
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
        self.assertEqual(
            ' | '.join(simple_project_expected_events),
            compile_run_results
        )

        # Check that .compiled directory is formed
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.json').is_file())

        # Check elements of manifest
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.json'))
        task01_refs = self._load_task_refs("task01.py", manifest)
        task02_refs = self._load_task_refs("task02.py", manifest)
        task03_refs = self._load_task_refs("task03.py", manifest)
        self.assertEqual([], task01_refs)
        self.assertEqual('task01.py', task02_refs)
        self.assertEqual([], task03_refs)

        # Set up wkdir for the next test case
        shutil.rmtree(Path(wkdir / '.compiled'))
        self._set_up_wkdir()

    def test_project_nested_task_dirs(self):
        """
        `prism compile` in a project with directories in the tasks folder
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '010_project_nested_task_dirs'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        if Path(wkdir / '.compiled').is_dir():
            shutil.rmtree(Path(wkdir / '.compiled'))

        args = ['compile']
        compile_run = self._run_prism(args)
        compile_run_results = compile_run.get_results()
        self.assertEqual(
            ' | '.join(simple_project_expected_events),
            compile_run_results
        )

        # Check that .compiled directory is formed
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.json').is_file())

        # Check elements of manifest
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.json'))
        extract_task01_refs = self._load_task_refs("extract/task01.py", manifest)
        extract_task02_refs = self._load_task_refs("extract/task02.py", manifest)
        load_task03_refs = self._load_task_refs("load/task03.py", manifest)
        task04_refs = self._load_task_refs("task04.py", manifest)
        self.assertEqual([], extract_task01_refs)
        self.assertEqual("extract/task01.py", extract_task02_refs)
        self.assertEqual("extract/task02.py", load_task03_refs)
        self.assertEqual("load/task03.py", task04_refs)

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
        self.assertEqual(
            ' | '.join(project_with_error_expected_events),
            compile_run_results
        )

        # Check that .compiled directory is not created
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertFalse(Path(wkdir / '.compiled' / 'manifest.json').is_file())

        # Set up wkdir for the next test case
        shutil.rmtree(Path(wkdir / '.compiled'))
        self._set_up_wkdir()
