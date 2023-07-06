"""
Unit testing for functions parsing the triggers.yml file.

Table of Contents:
- Imports
- Test case directory and paths
- Run task instance to call functions
- Test case class definition
"""

###########
# Imports #
###########

# Standard library imports
import os
from pathlib import Path
import unittest
import yaml

# Prism imports
from prism.infra.project import PrismProject
from prism.triggers import PrismTrigger, TriggerManager
import prism.exceptions


#################################
# Test case directory and paths #
#################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TEST_TRIGGER_YML = Path(TEST_CASE_WKDIR) / 'test_trigger_yml'
TRIGGERS_YML_TEST_YML = TEST_TRIGGER_YML / 'triggers.yml'
TRIGGERS_YML_BAD_INCLUDE = TEST_TRIGGER_YML / 'triggers_bad_include.yml'
TRIGGERS_YML_BAD_TRIGGERS = TEST_TRIGGER_YML / 'triggers_bad_triggers.yml'
TRIGGERS_YML_EXTERNAL_INCLUDE = TEST_TRIGGER_YML / 'triggers_external_include.yml'

# Prism project object
prism_project = PrismProject(
    project_dir=TEST_TRIGGER_YML,
    user_context={},
    which="run",
    filename="prism_project.py"
)
prism_project.setup()


###################
# Helper function #
###################

def _load_yml_spec(yml_path: Path, trigger_name: str):
    """
    Load spec associated with `trigger_name` from `yml_path`

    args:
        yml_path: Path to `triggers.yml`
        trigger_name: name of trigger
    returns:
        spec as a Dict
    """
    with open(yml_path, 'r') as f:
        full_triggers_yml = yaml.safe_load(f)
    return full_triggers_yml["triggers"][trigger_name]


##############################
# Test case class definition #
##############################

class TestTrigger(unittest.TestCase):

    def test_triggers(self):
        """
        Normal trigger structure
        """
        # The trigger instantiation should confirm that the trigger is the correct
        # structure.
        trigger_name = "test_trigger_good_structure"
        spec = _load_yml_spec(TRIGGERS_YML_TEST_YML, trigger_name)
        PrismTrigger(trigger_name, spec)

        # Trigger with no type
        trigger_name = "test_trigger_no_type"
        spec = _load_yml_spec(TRIGGERS_YML_TEST_YML, trigger_name)
        with self.assertRaises(prism.exceptions.InvalidTriggerException) as cm:
            PrismTrigger(trigger_name, spec)
        expected_msg = f"trigger `{trigger_name}` does not have a `type`"
        self.assertEqual(expected_msg, str(cm.exception))

        # Trigger with unexpected type
        trigger_name = "test_trigger_invalid_type"
        spec = _load_yml_spec(TRIGGERS_YML_TEST_YML, trigger_name)
        with self.assertRaises(prism.exceptions.InvalidTriggerException) as cm:
            PrismTrigger(trigger_name, spec)
        expected_msg = f"trigger `{trigger_name}` has an invalid type `other_type`"
        self.assertEqual(expected_msg, str(cm.exception))

    def test_include(self):
        """
        Paths in `include` are appropriately parsed
        """
        # Set the triggers_yml_path in PrismProject
        prism_project.triggers_yml_path = TRIGGERS_YML_TEST_YML
        manager = TriggerManager(
            triggers_yml_path=TRIGGERS_YML_TEST_YML,
            prism_project=prism_project
        )
        triggers_yml = manager.load_triggers_yml(manager.triggers_yml_path)
        expected_paths = [TEST_TRIGGER_YML]
        actual_paths = manager.get_include_paths(triggers_yml)
        self.assertEqual(expected_paths, actual_paths)

    def test_bad_include(self):
        """
        An `include` value that is not a list will throw an error
        """
        # Set the triggers_yml_path in PrismProject
        prism_project.triggers_yml_path = TRIGGERS_YML_BAD_INCLUDE
        manager = TriggerManager(
            triggers_yml_path=TRIGGERS_YML_BAD_INCLUDE,
            prism_project=prism_project
        )
        triggers_yml = manager.load_triggers_yml(manager.triggers_yml_path)
        with self.assertRaises(prism.exceptions.InvalidTriggerException) as cm:
            manager.check_triggers_yml_structure(triggers_yml)
        expected_message = "\n".join([
            "bad `include` format...use the following structure",
            "",
            "  include:",
            '    - "{{ Path(__file__).parent }}"',
            "    - <path 2>",
            "    - ..."
        ])
        self.assertEqual(expected_message, str(cm.exception))

    def test_bad_triggers(self):
        """
        A `triggers` value that is not a dictionary will throw an error
        """
        # Set the triggers_yml_path in PrismProject
        prism_project.triggers_yml_path = TRIGGERS_YML_BAD_TRIGGERS
        manager = TriggerManager(
            triggers_yml_path=TRIGGERS_YML_BAD_TRIGGERS,
            prism_project=prism_project
        )
        triggers_yml = manager.load_triggers_yml(manager.triggers_yml_path)
        with self.assertRaises(prism.exceptions.InvalidTriggerException) as cm:
            manager.check_triggers_yml_structure(triggers_yml)
        expected_message = "\n".join([
            "bad `triggers` format...use the following structure",
            "",
            "  triggers:",
            "    <trigger name>",
            "      type: function",
            "      function: ...",
            "      kwargs:",
            "      arg1: value1",
            "    <trigger_name>",
            "      type: prism_project",
            "      project_dir: ...",
            "      context:",
            "      var1: value2",
        ])
        self.assertEqual(expected_message, str(cm.exception))

    def test_external_include(self):
        """
        The paths under `include` are added to the project's sys.path
        """
        # Set the triggers_yml_path in PrismProject
        prism_project.triggers_yml_path = TRIGGERS_YML_BAD_TRIGGERS
        manager = TriggerManager(
            triggers_yml_path=TRIGGERS_YML_BAD_TRIGGERS,
            prism_project=prism_project
        )
        triggers_yml = manager.load_triggers_yml(manager.triggers_yml_path)
        with self.assertRaises(prism.exceptions.InvalidTriggerException) as cm:
            manager.check_triggers_yml_structure(triggers_yml)
        expected_message = "\n".join([
            "bad `triggers` format...use the following structure",
            "",
            "  triggers:",
            "    <trigger name>",
            "      type: function",
            "      function: ...",
            "      kwargs:",
            "      arg1: value1",
            "    <trigger_name>",
            "      type: prism_project",
            "      project_dir: ...",
            "      context:",
            "      var1: value2",
        ])
        self.assertEqual(expected_message, str(cm.exception))

    def test_sys_path(self):
        """
        Paths in `include` are added to the Prism project's sys.path.config
        """
        # Path of interest
        DUMMY_TASKS = Path(TEST_CASE_WKDIR) / 'dummy_tasks'
        print(prism_project.run_context['sys'].path)

        # In `prism_project.py`, we have `Path(__file__).parent` in `SYS_PATH_CONFIG`.
        # Therefore, `test_trigger_yml` should be in the project's sys.path.
        self.assertTrue(str(TEST_TRIGGER_YML) in prism_project.run_context['sys'].path)
        self.assertFalse(str(DUMMY_TASKS) in prism_project.run_context['sys'].path)

        # Create the TriggerManager
        manager = TriggerManager(
            triggers_yml_path=TRIGGERS_YML_EXTERNAL_INCLUDE,
            prism_project=prism_project
        )
        manager.check_trigger_components(prism_project.run_context)
        self.assertTrue(str(DUMMY_TASKS) in prism_project.run_context['sys'].path)

        # Cleanup
        prism_project.cleanup(prism_project.run_context)
        self.assertFalse(str(DUMMY_TASKS) in prism_project.run_context['sys'].path)

        # The original project directory was also removed
        self.assertFalse(str(TEST_TRIGGER_YML) in prism_project.run_context['sys'].path)

    def test_trigger_import_statement(self):
        """
        The `import_function` function produces the expected import statement to
        execute.
        """
        # Load the trigger YML so that we can create a PrismTrigger object
        yml = _load_yml_spec(
            TRIGGERS_YML_EXTERNAL_INCLUDE, 'test_trigger_good_structure'
        )
        trigger = PrismTrigger(
            'test_trigger_good_structure',
            yml
        )
        actual_import_statement = trigger.import_function(
            'test_trigger_good_structure',
            yml,
            {},
            "test"
        )
        expected_import_statement = "import test_fn"
        self.assertEqual(expected_import_statement, actual_import_statement)


# Cleanup
prism_project.cleanup(prism_project.run_context)
