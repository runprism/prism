"""
PrismCallback class

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Standard library imports
from pathlib import Path
from typing import Any, Dict, List

# Prism imports
from prism.event_managers.base import BaseEventManager
import prism.exceptions
import prism.logging
from prism.parsers import yml_parser
from prism.infra.project import PrismProject


####################
# Class definition #
####################

class PrismCallback:
    """
    PrismCallback class. Callbacks allow users to call specific functions when a project
    successfully executes or when a project fails.

    Callbacks operate similarly to profiles. They live in their own YAML file, and users
    can specify which callbacks to use on which callbacks in their prism_project.py
    file.

        CALLBACKS = {
            'on_failure': ["fn1_in_callbacks_yml", "fn2_in_callbacks_yml"],
            'on_success': ["fn3_in_callbacks_yml", "fn4_in_callbacks_yml"],
        }
    """

    def __init__(self,
        callback_name: str,
        callback_spec: Dict[Any, Any],
    ):
        self.name = callback_name
        self.spec = callback_spec

        # Callback type
        self.check_callback_structure(self.name, self.spec)
        self.callback_type = self.spec["type"]

    def check_callback_structure(self,
        callback_name: str,
        callback_spec: Dict[str, Any]
    ):
        """
        Confirm that a function callback meets the following requirements:

        - Has one top-level key, which is the callback name
        - Has three keys: `type`, `function`, and `kwargs`, where the first two are
          strings and the last is set of key-value pairs.

        args:
            callback_name: name of callback
            callback_spec: callback YML as a dictionary
        returns:
            True if function callback is value; False otherwise
        """
        # Must have a `type` parameter
        if "type" not in list(callback_spec.keys()):
            raise prism.exceptions.InvalidCallbackException(
                msg=f"callback `{callback_name}` does not have a `type`"
            )
        callback_type = callback_spec["type"]
        if callback_type not in ["function", "prism_project"]:
            raise prism.exceptions.InvalidCallbackException(
                msg=f"callback `{callback_name}` has an invalid type `{callback_type}`"
            )
        if callback_type == "function":
            valid_keys = ["type", "function", "kwargs"]
        else:
            valid_keys = ["type", "project_dir", "context"]

        # Check keys
        callback_keys = list(callback_spec.keys())
        flag_expected_keys = all(
            [k1 == k2 for k1, k2 in zip(callback_keys, valid_keys)]
        )
        if not flag_expected_keys:
            return flag_expected_keys

        # Check key types
        valid_key_types = [str, str, dict]
        for k, t in zip(valid_keys, valid_key_types):
            if not isinstance(callback_spec[k], t):
                return False

        # Otherwise, return True
        return True

    def import_function(self,
        callback_name: str,
        callback_spec: Dict[Any, Any],
        run_context: Dict[Any, Any]
    ):
        """
        For function callbacks, we may need to import the function from a module. Note
        that the user must ensure that the path to that module is contained in
        SYS_PATH_CONF.

        args:
            callback_name: name of callback
            callback_spec: callback as a dictionary
            run_context: dictionary with run context variables
        returns:
            ...
        """
        # Callback must be of type `function`
        if callback_spec["type"] != "function":
            raise prism.exceptions.InvalidTriggerException(
                message=f"callback `{callback_name}` is not of type `function`"
            )

        # Since it's of type function, we know that the spec will have a key `function`
        fn = callback_spec["function"]
        fn_split = fn.split('.')[:-1]

        # If there is no parent module specified, then the function must be specified in
        # prism_project.py
        if len(fn_split) == 1:
            return
        else:
            exec(f"import {'.'.join(fn_split)}", run_context)

    def exec(self,
        callback_name: str,
        callback_spec: Dict[Any, Any],
        run_context: Dict[Any, Any]
    ):
        """
        Execute the callback

        args:
            callback_name: name of callback
            callback_spec: callback as a dictionary
            run_context: dictionary with run context variables
        returns:
            ...
        """
        self.import_function(callback_name, callback_spec, run_context)

    def execute_callback(self,
        run_context: Dict[Any, Any]
    ):
        """
        Execute the callback specified by `callback_name`

        args:
            callback_name: name of callback
            callback_spec: callback as a dictionary
            run_context: dictionary with run context variables
        returns:
            ...
        """
        self.import_function(self.name, self.spec, run_context)
        if self.spec["type"] == "function":
            fn = self.spec["function"]
            kwargs_dict = self.spec["kwargs"]
            exec(f"{fn}(**{kwargs_dict})", run_context)


class CallbackManager:
    """
    CallbackManager. This class is responsible for parsing the callback.yml file.
    """

    def __init__(self,
        callbacks_yml_path: Path,
        prism_project: PrismProject,
    ):
        parser = yml_parser.YamlParser(callbacks_yml_path)
        self.callbacks_yml = parser.parse()
        self.prism_project = prism_project

        # Check the callbacks_yml structure
        self.check_callbacks_yml_structure(self.callbacks_yml)
        self.callbacks = self.callbacks_yml['callbacks']

        # This object will only be instantiated after the PrismProject has been setup.
        if not (
            hasattr(self.prism_project, "on_success_callbacks")
            and hasattr(self.prism_project, "on_failure_callbacks")  # noqa: W503
        ):
            raise prism.exceptions.RuntimeException(
                message="PrismProject has not been properly setup!"
            )

        self.on_success_callbacks = [
            PrismCallback(name, self.callbacks[name]) for name in self.prism_project.on_success_callbacks  # noqa: E501
        ]
        self.on_failure_callbacks = [
            PrismCallback(name, self.callbacks[name]) for name in self.prism_project.on_failure_callbacks  # noqa: E501
        ]

    def check_callbacks_yml_structure(self,
        callbacks_yml: Dict[str, Any]
    ):
        """
        Confirm that the callbacks.yml file has the following structure:
            callbacks:
              <callback name>
                  type: function
                  function:
                  kwargs:
                    arg1: value1
              <callback_name>
                  type: prism_project
                  project_dir:
                  context:

        That is:
        - Only one top-level key `callbacks`
        - Each callback gets its own second-level key
        - Function callbacks have three keys: `type`, `function`, and `kwargs`, where
          the first two are strings and the last is set of key-value pairs
        - Prism project callbacks have three keys: `type`, `project_dir`, and `context`,
          where the first two are strings and the last is set of key-value pairs

        args:
            callbacks_yml: `callbacks.yml` file as a dictionary
        returns:
            True if callbacks_yml satisfies all the conditions above
        raises:
            InvalidCallbackException if callbacks_yml fails any of the conditions above
        """

        # Check top-level keys
        top_level_keys = list(callbacks_yml.keys())
        if len(top_level_keys) > 1:
            raise prism.exceptions.InvalidCallbackException(
                message="too many top-level keys; should only be `callbacks`"
            )
        top_level_key = top_level_keys[0]
        allowed_key = 'callbacks'
        if top_level_key != allowed_key:
            raise prism.exceptions.InvalidCallbackException(
                message=f"invalid top-level key `{top_level_key}`; should only be `{allowed_key}`"  # noqa: E501
            )

        # If nothing is raised, then return True
        return True

    def exec(self,
        callback_type: str,
        full_tb: bool,
        event_list: List[prism.logging.Event],
        run_context: Dict[Any, Any]
    ):
        """
        Execute the callbacks for `callback_type`

        args:
            callback_type: either `on_success` or `on_failure`
            full_tb: boolean indicating wheter to display full traceback
            event_list: event list
            run_context: dictionary with run context variables
        returns:
            ...
        """
        if callback_type not in ["on_success", "on_failure"]:
            raise prism.exceptions.RuntimeException(
                message=f"invalid callback type `{callback_type}`, must be either `on_success` or `on_failure`"  # noqa: E501
            )

        # Keep track of outputs
        for cb in getattr(self, f"{callback_type}_callbacks"):

            # Event manager
            cb_event_manager = BaseEventManager(
                idx=None,
                total=None,
                name=f'callback {cb.name}',
                full_tb=full_tb,
                func=cb.execute_callback
            )
            cb_event_manager_output = cb_event_manager.manage_events_during_run(
                event_list=event_list,
                run_context=run_context
            )
            event_list.append(cb_event_manager_output.event_list)
            if cb_event_manager_output.outputs == 0:
                ev = cb_event_manager_output.event_to_fire
                prism.logging.fire_console_event(ev)

        return event_list
