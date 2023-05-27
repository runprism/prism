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

# Prism imports
import prism.tests.integration.integration_test_class as integration_test_class


#################################
# Test case directory and paths #
#################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TEST_PROJECTS = Path(TEST_CASE_WKDIR) / 'test_projects'

# Project directories
P005_SIMPLE_PROJECT_NO_NULL = Path(TEST_PROJECTS / '005_simple_project_no_null')


###################
# Expected events #
###################

SUCCESS_EXPECTED_CREATE_START_EVENTS = [
    'SeparatorEvent',
    'TaskRunEvent',
    'CurrentProjectDirEvent',
    'EmptyLineEvent',
    'ExecutionEvent - parsing prism_project.py - RUN',
    'ExecutionEvent - parsing prism_project.py - DONE',
    'EmptyLineEvent',
]

SUCCESS_EXPECTED_CREATE_TRIGGER_EVENTS = [
    'CreatingTriggersEvent',
]

SUCCESS_EXPECTED_CREATE_TASK_EVENTS = [
    'CreatingTasksEvent',
]

SUCCESS_EXPECTED_CREATE_END_EVENTS = [
    'EmptyLineEvent',
    'TaskSuccessfulEndEvent',
    'SeparatorEvent',
]

TASK_ALREADY_EXISTS_END_EVENTS = [
    'EmptyLineEvent',
    'PrismExceptionErrorEvent',
    'SeparatorEvent'
]


##############################
# Test case class definition #
##############################

class TestCreate(integration_test_class.IntegrationTestCase):

    def test_create_trigger(self):
        """
        Create `triggers.yml` file
        """
        # Change directory to 005_simple_project_with_profile
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '005_simple_project_no_null'
        os.chdir(wkdir)

        # First, check that `triggers.yml` does not exist in the project
        self.assertFalse(Path(wkdir / 'triggers.yml').is_file())

        # Run
        args = ['create', 'trigger', '--type', 'function']
        create_task_run = self._run_prism(args)

        # `triggers.yml` file was found
        self.assertTrue(Path(wkdir / 'triggers.yml').is_file())
        os.unlink(Path(wkdir / 'triggers.yml'))

        # Expected events
        expected_events = SUCCESS_EXPECTED_CREATE_START_EVENTS \
            + SUCCESS_EXPECTED_CREATE_TRIGGER_EVENTS \
            + SUCCESS_EXPECTED_CREATE_END_EVENTS
        self.assertEqual(' | '.join(expected_events), create_task_run.get_results())

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_create_task(self):
        """
        Create new tasks
        """
        # Change directory to 005_simple_project_with_profile
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '005_simple_project_no_null'
        os.chdir(wkdir)

        # First, check that `dummy_task.py` does not exist in the project
        self.assertFalse(Path(wkdir / 'modules' / 'dummy_task.py').is_file())

        # ------------------------------------------------------------------------------
        # Run
        args = ['create', 'task', '--type', 'python', '--name', 'dummy_task']
        create_task_run = self._run_prism(args)

        # `triggers.yml` file was found
        self.assertTrue(Path(wkdir / 'modules' / 'dummy_task.py').is_file())

        # Expected events
        expected_events = SUCCESS_EXPECTED_CREATE_START_EVENTS \
            + SUCCESS_EXPECTED_CREATE_TASK_EVENTS \
            + SUCCESS_EXPECTED_CREATE_END_EVENTS
        self.assertEqual(' | '.join(expected_events), create_task_run.get_results())

        # Get the class name
        with open(Path(wkdir / 'modules' / 'dummy_task.py'), 'r') as f:
            dummy_task_py = f.read()
        self.assertTrue("class DummyTask(prism.task.PrismTask):" in dummy_task_py)

        # ------------------------------------------------------------------------------
        # Try creating `dummy_task.py` again. It should raise an error
        run_twice = self._run_prism(args)
        expected_events = SUCCESS_EXPECTED_CREATE_START_EVENTS \
            + SUCCESS_EXPECTED_CREATE_TASK_EVENTS \
            + TASK_ALREADY_EXISTS_END_EVENTS
        self.assertEqual(' | '.join(expected_events), run_twice.get_results())

        # Remove task
        os.unlink(Path(wkdir / 'modules' / 'dummy_task.py'))

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_create_decorated_task(self):
        """
        Created a task using the --decorated flag. This will create a class that has
        a decorated function rather than a class.
        """
        # Change directory to 005_simple_project_with_profile
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '005_simple_project_no_null'
        os.chdir(wkdir)

        # First, check that `dummy_task.py` does not exist in the project
        self.assertFalse(Path(wkdir / 'modules' / 'dummy_task.py').is_file())

        # ------------------------------------------------------------------------------
        # Run
        args = [
            'create', 'task', '--type', 'python', '--name', 'dummy_task', '--decorated'
        ]
        create_task_run = self._run_prism(args)

        # `triggers.yml` file was found
        self.assertTrue(Path(wkdir / 'modules' / 'dummy_task.py').is_file())

        # Expected events
        expected_events = SUCCESS_EXPECTED_CREATE_START_EVENTS \
            + SUCCESS_EXPECTED_CREATE_TASK_EVENTS \
            + SUCCESS_EXPECTED_CREATE_END_EVENTS
        self.assertEqual(' | '.join(expected_events), create_task_run.get_results())

        # Get the class name
        with open(Path(wkdir / 'modules' / 'dummy_task.py'), 'r') as f:
            dummy_task_py = f.read()
        self.assertTrue("class DummyTask(prism.task.PrismTask):" not in dummy_task_py)
        decorated_string = "\n".join([
            "@task(",
            "    retries=0,",
            "    retry_delay_seconds=0,",
            "    targets=[",
            "        target(...)",
            "    ]",
            ")"
        ])
        self.assertTrue(decorated_string in dummy_task_py)
        self.assertTrue("def dummy_task(tasks, hooks):" in dummy_task_py)

        # Remove task
        os.unlink(Path(wkdir / 'modules' / 'dummy_task.py'))

        # Set up wkdir for the next test case
        self._set_up_wkdir()
