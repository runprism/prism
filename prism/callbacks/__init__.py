"""
PrismCallback class

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Prism imports
from prism.cli.base import TaskRunReturnResult
from prism.event_managers.base import BaseEventManager
import prism.exceptions
import prism.logging
from prism.parsers import yml_parser
from prism.infra.project import PrismProject

# Standard library imports
import jinja2
from pathlib import Path
from typing import Any, Dict, List, Optional


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
                message=f"callback `{callback_name}` does not have a `type`"
            )
        callback_type = callback_spec["type"]
        if callback_type not in ["function", "prism_project"]:
            raise prism.exceptions.InvalidCallbackException(
                message=f"callback `{callback_name}` has an invalid type `{callback_type}`"  # noqa: E501
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
            raise prism.exceptions.InvalidCallbackException(
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
        callbacks_dir: Optional[Path],
        prism_project: PrismProject,
    ):
        self.prism_project = prism_project
        if callbacks_dir is None:
            self.callbacks_yml_path = None
        else:
            self.callbacks_yml_path = Path(callbacks_dir) / 'callbacks.yml'
        self.flag_has_callbacks_yml_path = self.callbacks_yml_path is not None

        # This object will only be instantiated after the PrismProject has been
        # setup. `on_success_callbacks` and `on_failure_callbacks` will always defined.
        if not (
            hasattr(self.prism_project, "on_success_callbacks")
            and hasattr(self.prism_project, "on_failure_callbacks")  # noqa: W503
        ):
            raise prism.exceptions.RuntimeException(
                message="PrismProject has not been properly setup!"
            )

        self.flag_has_callbacks = (
            self.prism_project.on_success_callbacks != []
            or self.prism_project.on_failure_callbacks != []  # noqa: W503
        )

        # Did we default to the project directory?
        self.defaulted_to_project_dir = False

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

    def create_callback_instances(self,
        callbacks_yml_path: Optional[Path],
        callback_names: List[str],
        callbacks_specs: Dict[str, Dict[Any, Any]]
    ) -> List[PrismCallback]:
        """
        Create PrismCallback objects

        args:
            callbacks_yml_path: path to callbacks.yml
            callback_names: list of callback names
            callback_specs: dictionary of callback name --> callback_spec
        returns:
            list of PrismCallback objects
        """
        callback_objs = []
        for name in callback_names:
            try:
                callback_objs.append(
                    PrismCallback(name, callbacks_specs[name])
                )
            except KeyError:
                raise prism.exceptions.InvalidCallbackException(
                    message=f"callback `{name}` not found in `{callbacks_yml_path}`"
                )
        return callback_objs

    def check_callback_components(self):
        """
        Confirm that all the components for callbacks (i.e., the YAML file, the
        variables in prism_project.py, etc.) are properly defined.
        """
        # If the user has not specified any callbacks, then set the success/failure
        # callbacks to empty lists
        if not self.flag_has_callbacks:
            self.callbacks_yml = {}
            self.callbacks = {}
            self.on_success_callbacks = []
            self.on_failure_callbacks = []

        # If the user specified callbacks but not a callbacks directory, throw a warning
        # and default to the project directory.
        else:

            if not self.flag_has_callbacks_yml_path:
                self.defaulted_to_project_dir = True
                self.callbacks_yml_path = self.prism_project.project_dir / 'callbacks.yml'  # noqa: E501

            if not isinstance(self.callbacks_yml_path, Path):
                raise prism.exceptions.InvalidCallbackException(
                    message="something went wrong with callbacks YAML path"
                )

            # If the callbacks path isn't actually a file, throw an error
            if not self.callbacks_yml_path.is_file():
                raise prism.exceptions.InvalidCallbackException(
                    message=f"could not find `{self.callbacks_yml_path}`"
                )

            # Parse the YAML file. If it doesn't exist, then throw an error.
            try:
                parser = yml_parser.YamlParser(self.callbacks_yml_path)
                self.callbacks_yml = parser.parse()
            except jinja2.exceptions.TemplateNotFound:
                raise prism.exceptions.InvalidCallbackException(
                    message=f"could not find `{self.callbacks_yml_path}`"
                )

            # If parsed file is empty, throw an error
            if self.callbacks_yml == {}:
                raise prism.exceptions.InvalidCallbackException(
                    message=f"file at `{self.callbacks_yml_path}` is empty"
                )

            # Check the callbacks_yml structure
            self.check_callbacks_yml_structure(self.callbacks_yml)
            self.callbacks = self.callbacks_yml['callbacks']

            # Success callbacks
            self.on_success_callbacks = self.create_callback_instances(
                self.callbacks_yml_path,
                self.prism_project.on_success_callbacks,
                self.callbacks
            )

            # Failure callbacks
            self.on_failure_callbacks = self.create_callback_instances(
                self.callbacks_yml_path,
                self.prism_project.on_failure_callbacks,
                self.callbacks
            )

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
            full_tb: boolean indicating whether to display full traceback
            event_list: event list
            run_context: dictionary with run context variables
        returns:
            ...
        """
        if callback_type not in ["on_success", "on_failure"]:
            raise prism.exceptions.RuntimeException(
                message=f"invalid callback type `{callback_type}`, must be either `on_success` or `on_failure`"  # noqa: E501
            )

        # Set up the callbacks
        setup_event_manager = BaseEventManager(
            idx=None,
            total=None,
            name='setting up callbacks',
            full_tb=full_tb,
            func=self.check_callback_components
        )
        setup_event_manager_output = setup_event_manager.manage_events_during_run(
            event_list=event_list,
            fire_exec_events=False,
            fire_empty_line_events=False
        )
        event_list.extend(setup_event_manager_output.event_list)
        if setup_event_manager_output.outputs == 0:
            event_list = prism.logging.fire_empty_line_event(event_list)
            event_list = prism.logging.fire_console_event(
                setup_event_manager_output.event_to_fire, event_list, log_level='error'
            )
            return event_list

        # Callback header events
        callbacks_to_exec = getattr(self, f"{callback_type}_callbacks")
        if len(callbacks_to_exec) > 0:
            event_list = prism.logging.fire_console_event(
                prism.logging.CallbacksHeaderEvent(),
                event_list,
            )

            # Warning to indicate we defaulted to project directory
            if self.defaulted_to_project_dir:
                event_list = prism.logging.fire_console_event(
                    prism.logging.CallbacksPathNotDefined(),
                    log_level='warn'
                )

        # Execute callbacks
        cb_has_error = False
        for cb in callbacks_to_exec:
            cb_event_manager = BaseEventManager(
                idx=None,
                total=None,
                name=f'callback {cb.name}',
                full_tb=full_tb,
                func=cb.execute_callback
            )
            cb_event_manager_output = cb_event_manager.manage_events_during_run(
                event_list=event_list,
                run_context=run_context,
                fire_empty_line_events=False
            )
            event_list.extend(cb_event_manager_output.event_list)

            # Fire the error event. Don't return after the first error, because we want
            # to execute all callbacks.
            if cb_event_manager_output.outputs == 0:
                cb_has_error = True
                event_list = prism.logging.fire_empty_line_event(event_list)
                event_list = prism.logging.fire_console_event(
                    cb_event_manager_output.event_to_fire, event_list, log_level='error'
                )
        return TaskRunReturnResult(
            event_list, cb_has_error
        )
