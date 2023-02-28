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
from prism.triggers import PrismTrigger
import prism.exceptions


#################################
# Test case directory and paths #
#################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TRIGGERS_YML_TEST_YML = Path(TEST_CASE_WKDIR) / 'test_trigger_yml' / 'triggers.yml'


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
