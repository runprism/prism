"""
Mixin class for the CreateTrigger task

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Standard library imports
import yaml
import shutil
from pathlib import Path
from typing import Any, Dict

# Prism-specific imports
import prism.cli.base
import prism.cli.compile
import prism.exceptions
import prism.constants
import prism.logging
from prism.templates.triggers import TRIGGERS_TEMPLATE_DIR as triggers_template_dir
from prism.triggers import PrismTrigger


####################
# Class definition #
####################

class CreateTriggersMixin():
    """
    Mixin for CreateTrigger task
    """

    def create_directory(self,
        path: str
    ):
        """
        Create a directory at `path` if it doesn't exist.

        args:
            path: path to create a directory
        returns:
            None
        """
        if not Path(path).is_dir():
            p = Path(path)
            p.mkdir(parents=True, exist_ok=True)

    def update_yml(self,
        base_triggers_yml: Dict[str, Any],
        new_trigger: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Dump `new_trigger` into `base_triggers_yml`. Note that `new_trigger` should only
        have one key

        args:
            base_triggers_yml: YML file to update
            new_trigger: new dictionary to dump into YML
        returns:
            base_triggers_yml dictionary with new_trigger appended on
        """
        # If the triggers.yml is empty, then throw an error
        if base_triggers_yml == {}:
            raise prism.exceptions.InvalidTriggerException(
                message="`triggers.yml` is empty"
            )

        # `triggers.yml` should have a single key : `triggers`
        keys = list(base_triggers_yml.keys())
        values = list(base_triggers_yml.values())
        if len(keys) != 1 or len(values) != 1:
            raise prism.exceptions.InvalidTriggerException(
                message="invalid `triggers.yml`"
            )

        # The values for `base_triggers_yml` should be a dictionary
        values_dict = list(base_triggers_yml.values())[0]
        if not isinstance(values_dict, dict):
            raise prism.exceptions.InvalidTriggerException(
                message="invalid `triggers.yml`; values should be nested dictionaries"
            )

        # The new dictionary should have a single key representing the trigger to add
        new_trigger_keys = list(new_trigger.keys())
        new_trigger_values = list(new_trigger.values())
        if len(new_trigger_keys) != 1 or len(new_trigger_values) != 1:
            raise prism.exceptions.InvalidTriggerException(
                message="new trigger not properly formatted"
            )

        # This will automatically check that the trigger is properly formatted
        trigger_name = new_trigger_keys[0]
        new_trigger_spec = new_trigger_values[0]
        PrismTrigger(trigger_name, new_trigger_spec)

        # Add the trigger
        base_triggers_yml['triggers'][trigger_name] = new_trigger_spec

        # Return the revised base_triggers_yml
        return base_triggers_yml

    def create_trigger_from_template(self,
        type: str,
        triggers_filepath: Path
    ):
        f"""
        Create a triggers.yml file using the template.

        args:
            type: trigger type; one of {prism.constants.VALID_TRIGGER_TYPES}
            triggers_filepath: location to keep the the trigger.yml file
        returns:
            None
        """

        # We only ever call this function after confirming that the trigger.yml file
        # does not exist and that the type is valid.
        triggers_template_path = Path(triggers_template_dir) / f'{type}.yml'
        shutil.copyfile(triggers_template_path, triggers_filepath)

    def create_trigger(self,
        trigger_type: str,
        triggers_filepath: Path
    ):
        """
        Create a trigger for the inputted `trigger_type`

        args:
            trigger_type: trigger type (either `function` or `prism_project`)
            triggers_filepath: path to triggers.yml
        returns:
            profile YML with added profile of type `trigger_type`
        """
        # If the profile doesn't exist, then create it
        if not triggers_filepath.is_file():
            self.create_trigger_from_template(trigger_type, triggers_filepath)
            return

        # If the profile does exist, then update the profile based on the profile type
        with open(triggers_filepath) as f:
            trigger_base_yml = yaml.safe_load(f)
        f.close()

        template_path = Path(triggers_template_dir) / f'{trigger_type}.yml'
        with open(template_path, 'r') as f:
            template_yml = yaml.safe_load(f)
        f.close()

        if trigger_type in prism.constants.VALID_TRIGGER_TYPES:
            new_trigger = template_yml['triggers']
        else:
            raise prism.exceptions.InvalidTriggerException(
                message=f"new trigger_type is invalid must be one of `{prism.constants.VALID_TRIGGER_TYPES:}`"  # noqa: E501
            )

        # Update the template_yml
        trigger_base_yml_updated = self.update_yml(trigger_base_yml, new_trigger)

        # Save the new profile name
        with open(triggers_filepath, 'w') as f:
            yaml.dump(trigger_base_yml_updated, f, sort_keys=False)
        f.close()
