"""
PrismTrigger class

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
import prism.prism_logging
from prism.parsers import yml_parser
from prism.infra.project import PrismProject
from prism.constants import VALID_TRIGGER_TYPES

# Standard library imports
import jinja2
from pathlib import Path
from typing import Any, Dict, List, Optional


####################
# Class definition #
####################

class PrismTrigger:
    """
    PrismTrigger class. Triggers allow users to call specific functions when a project
    successfully executes or when a project fails.

    Triggers operate similarly to profiles. They live in their own YAML file, and users
    can specify which triggers to use on which triggers in their prism_project.py
    file.

        TRIGGERS = {
            'on_failure': ["fn1_in_triggers_yml", "fn2_in_triggers_yml"],
            'on_success': ["fn3_in_triggers_yml", "fn4_in_triggers_yml"],
        }
    """

    def __init__(self,
        trigger_name: str,
        trigger_spec: Dict[Any, Any],
    ):
        self.name = trigger_name
        self.spec = trigger_spec

        # Trigger type
        self.check_trigger_structure(self.name, self.spec)
        self.trigger_type = self.spec["type"]

    def check_trigger_structure(self,
        trigger_name: str,
        trigger_spec: Dict[str, Any]
    ):
        """
        Confirm that a function trigger meets the following requirements:

        - Has one top-level key, which is the trigger name
        - Has three keys: `type`, `function`, and `kwargs`, where the first two are
          strings and the last is set of key-value pairs.

        args:
            trigger_name: name of trigger
            trigger_spec: trigger YML as a dictionary
        returns:
            True if function trigger is value; False otherwise
        """
        # Must have a `type` parameter
        if "type" not in list(trigger_spec.keys()):
            raise prism.exceptions.InvalidTriggerException(
                message=f"trigger `{trigger_name}` does not have a `type`"
            )
        trigger_type = trigger_spec["type"]
        if trigger_type not in VALID_TRIGGER_TYPES:
            raise prism.exceptions.InvalidTriggerException(
                message=f"trigger `{trigger_name}` has an invalid type `{trigger_type}`"  # noqa: E501
            )
        if trigger_type == "function":
            valid_keys = ["type", "function"]
            optional_keys = ["kwargs"]
        else:
            valid_keys = ["type", "project_dir"]
            optional_keys = ["context"]

        # Check keys
        trigger_keys = list(trigger_spec.keys())
        flag_expected_keys = all(
            [
                k1 == k2 or k1 in optional_keys for k1, k2 in zip(
                    sorted(trigger_keys), sorted(valid_keys)
                )
            ]
        )
        if not flag_expected_keys:
            return flag_expected_keys

        # Check key types
        valid_key_types = [str, str]
        optional_key_types = [dict]
        for k, t in zip(valid_keys, valid_key_types):
            if not isinstance(trigger_spec[k], t):
                return False
        for k, t in zip(optional_keys, optional_key_types):  # type: ignore
            if k in trigger_keys:
                if not isinstance(trigger_spec[k], t):
                    return False

        # Otherwise, return True
        return True

    def import_function(self,
        trigger_name: str,
        trigger_spec: Dict[Any, Any],
        run_context: Dict[Any, Any],
        mode="prod"
    ) -> Optional[str]:
        """
        For function triggers, we may need to import the function from a model. Note
        that the user must ensure that the path to that model is contained in
        SYS_PATH_CONF.

        args:
            trigger_name: name of trigger
            trigger_spec: trigger as a dictionary
            run_context: dictionary with run context variables
        returns:
            ...
        """
        # Trigger must be of type `function`
        if trigger_spec["type"] != "function":
            raise prism.exceptions.InvalidTriggerException(
                message=f"trigger `{trigger_name}` is not of type `function`"
            )

        # Since it's of type function, we know that the spec will have a key `function`
        fn = trigger_spec["function"]
        fn_split = fn.split('.')

        # There must be a parent model specified. If there isn't, then something is
        # wrong.
        if len(fn_split) == 1:
            raise prism.exceptions.RuntimeException(
                message=f"no parent model specified for trigger `{trigger_name}`"
            )
        else:
            if mode == "prod":
                exec(f"import {'.'.join(fn_split[:-1])}", run_context)
                return None
            else:
                return f"import {'.'.join(fn_split[:-1])}"

    def execute_trigger(self,
        run_context: Dict[Any, Any]
    ):
        """
        Execute the trigger specified by `trigger_name`

        args:
            trigger_name: name of trigger
            trigger_spec: trigger as a dictionary
            run_context: dictionary with run context variables
        returns:
            ...
        """
        self.import_function(self.name, self.spec, run_context)
        if self.spec["type"] == "function":
            fn = self.spec["function"]
            if "kwargs" in list(self.spec.keys()):
                kwargs_dict = self.spec["kwargs"]
                exec(f"{fn}(**{kwargs_dict})", run_context)
            else:
                exec(f"{fn}()", run_context)


class TriggerManager:
    """
    TriggerManager. This class is responsible for parsing the trigger.yml file.
    """

    def __init__(self,
        triggers_yml_path: Optional[Path],
        prism_project: PrismProject,
    ):
        self.prism_project = prism_project
        self.triggers_yml_path = triggers_yml_path
        self.flag_has_triggers_yml_path = self.triggers_yml_path is not None

        # This object will only be instantiated after the PrismProject has been
        # setup. `on_success_triggers` and `on_failure_triggers` will always defined.
        if not (
            hasattr(self.prism_project, "on_success_triggers")
            and hasattr(self.prism_project, "on_failure_triggers")  # noqa: W503
        ):
            raise prism.exceptions.RuntimeException(
                message="PrismProject has not been properly setup!"
            )

        self.flag_has_triggers = (
            self.prism_project.on_success_triggers != []
            or self.prism_project.on_failure_triggers != []  # noqa: W503
        )

        # Did we default to the project directory?
        self.defaulted_to_project_dir = False

    def check_triggers_yml_structure(self,
        triggers_yml: Dict[str, Any]
    ):
        """
        Confirm that the triggers.yml file has the following structure:
            triggers:
              <trigger name>
                  type: function
                  function:
                  kwargs:
                    arg1: value1
              <trigger_name>
                  type: prism_project
                  project_dir:
                  context:

        That is:
        - Only one top-level key `triggers`
        - Each trigger gets its own second-level key
        - Function triggers have three keys: `type`, `function`, and `kwargs`, where
          the first two are strings and the last is set of key-value pairs
        - Prism project triggers have three keys: `type`, `project_dir`, and `context`,
          where the first two are strings and the last is set of key-value pairs

        args:
            triggers_yml: `triggers.yml` file as a dictionary
        returns:
            True if triggers_yml satisfies all the conditions above
        raises:
            InvalidTriggerException if triggers_yml fails any of the conditions above
        """
        # Warning events
        warning_events = []

        # Expected keys
        expected_keys = ["include", "triggers"]

        # Check top-level keys
        top_level_keys = list(triggers_yml.keys())

        # Identify keys that are not in the expected keys
        unexpected_keys = list(set(top_level_keys) - set(expected_keys))
        if len(unexpected_keys) > 0:
            warning_events.append(
                prism.prism_logging.UnexpectedTriggersYmlKeysEvent(unexpected_keys)
            )

        # We definitely need the triggers YML to have `triggers`. The `include` key is
        # optional.
        if "triggers" not in top_level_keys:
            raise prism.exceptions.InvalidTriggerException(
                message="could not find `triggers` key in triggers YML file"
            )

        # The value of `triggers` must be a dictionary
        if not isinstance(triggers_yml["triggers"], dict):
            msg = "\n".join([
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
            raise prism.exceptions.InvalidTriggerException(message=msg)

        # The value of `include` must be a list
        if "include" in top_level_keys:
            if not isinstance(triggers_yml["include"], list):
                raise prism.exceptions.InvalidTriggerException(
                    message="\n".join([
                        "bad `include` format...use the following structure",
                        "",
                        "  include:",
                        '    - "{{ Path(__file__).parent }}"',
                        '    - <path 2>',
                        '    - ...'
                    ])
                )

        # If nothing is raised, then return True
        return warning_events

    def create_trigger_instances(self,
        triggers_yml_path: Optional[Path],
        trigger_names: List[str],
        triggers_specs: Dict[str, Dict[Any, Any]]
    ) -> List[PrismTrigger]:
        """
        Create PrismTrigger objects

        args:
            triggers_yml_path: path to triggers.yml
            trigger_names: list of trigger names
            trigger_specs: dictionary of trigger name --> trigger_spec
        returns:
            list of PrismTrigger objects
        """
        trigger_objs = []
        for name in trigger_names:
            try:
                trigger_objs.append(
                    PrismTrigger(name, triggers_specs[name])
                )
            except KeyError:
                raise prism.exceptions.InvalidTriggerException(
                    message=f"trigger `{name}` not found in `{triggers_yml_path}`"
                )
        return trigger_objs

    def load_triggers_yml(self, triggers_yml_path: Path):
        """
        Parse the YAML file. If it doesn't exist, then throw an error.

        args:
            triggers_yml_path: path to trigger YML file
        returns:
            trigger YML file as a dictionary
        raises:
            prism.exceptions.InvalidTriggerException if the file is not found
        """
        try:
            parser = yml_parser.YamlParser(
                triggers_yml_path,
                self.prism_project
            )
            triggers_yml = parser.parse()
            return triggers_yml
        except jinja2.exceptions.TemplateNotFound:
            raise prism.exceptions.InvalidTriggerException(
                message=f"could not find `{self.triggers_yml_path}`"
            )
        except Exception as e:
            raise e

    def get_include_paths(self, triggers_yml: Dict[str, Any]) -> List[Path]:
        """
        Get the paths listed under `include`
        """
        if 'include' in triggers_yml.keys():
            if len(triggers_yml['include']) > 0:
                return [Path(_p) for _p in triggers_yml["include"]]

        # Otherwise, return an empty list
        return []

    def check_trigger_components(self, run_context: Dict[Any, Any]):
        """
        Confirm that all the components for triggers (i.e., the YAML file, the
        variables in prism_project.py, etc.) are properly defined.
        """
        # If the user has not specified any triggers, then set the success/failure
        # triggers to empty lists
        if not self.flag_has_triggers:
            self.triggers_yml = {}
            self.triggers = {}
            self.on_success_triggers = []
            self.on_failure_triggers = []

        # If the user specified triggers but not a triggers directory, throw a warning
        # and default to the project directory.
        else:

            if not self.flag_has_triggers_yml_path:
                self.defaulted_to_project_dir = True
                self.triggers_yml_path = self.prism_project.project_dir / 'triggers.yml'  # noqa: E501

            if not isinstance(self.triggers_yml_path, Path):
                raise prism.exceptions.InvalidTriggerException(
                    message="something went wrong with triggers YML path"
                )

            # If the triggers path isn't actually a file, throw an error
            if not self.triggers_yml_path.is_file():
                raise prism.exceptions.InvalidTriggerException(
                    message=f"could not find `{self.triggers_yml_path}`"
                )

            # Parse the YAML file. If it doesn't exist, then throw an error.
            self.triggers_yml = self.load_triggers_yml(self.triggers_yml_path)

            # If parsed file is empty, throw an error
            if self.triggers_yml == {}:
                raise prism.exceptions.InvalidTriggerException(
                    message=f"file at `{self.triggers_yml_path}` is empty"
                )

            # Check the triggers_yml structure
            warning_events = self.check_triggers_yml_structure(self.triggers_yml)
            self.triggers = self.triggers_yml['triggers']

            # Add the paths in `include` to the project's sys.path. Add the paths to
            # sys.path
            include_paths: List[Path] = self.get_include_paths(self.triggers_yml)
            self.prism_project.sys_path_engine.add_paths_to_sys_path(
                include_paths,
                run_context
            )

            # Add the paths to the project's sys.path.config. This will allow us
            # to properly remove them.
            for _p in include_paths:
                if _p not in self.prism_project.sys_path_config:
                    self.prism_project.sys_path_config.append(Path(_p))

            # Success triggers
            self.on_success_triggers = self.create_trigger_instances(
                self.triggers_yml_path,
                self.prism_project.on_success_triggers,
                self.triggers
            )

            # Failure triggers
            self.on_failure_triggers = self.create_trigger_instances(
                self.triggers_yml_path,
                self.prism_project.on_failure_triggers,
                self.triggers
            )

            # Return warning events
            return warning_events

    def exec(self,
        trigger_type: str,
        full_tb: bool,
        event_list: List[prism.prism_logging.Event],
        run_context: Dict[Any, Any]
    ):
        """
        Execute the triggers for `trigger_type`

        args:
            trigger_type: either `on_success` or `on_failure`
            full_tb: boolean indicating whether to display full traceback
            event_list: event list
            run_context: dictionary with run context variables
        returns:
            ...
        """
        if trigger_type not in ["on_success", "on_failure"]:
            raise prism.exceptions.RuntimeException(
                message=f"invalid trigger type `{trigger_type}`, must be either `on_success` or `on_failure`"  # noqa: E501
            )

        # Set up the triggers
        setup_event_manager = BaseEventManager(
            idx=None,
            total=None,
            name='setting up triggers',
            full_tb=full_tb,
            func=self.check_trigger_components
        )
        setup_event_manager_output = setup_event_manager.manage_events_during_run(
            event_list=event_list,
            fire_exec_events=False,
            fire_empty_line_events=False,
            run_context=run_context
        )
        event_list = setup_event_manager_output.event_list
        if setup_event_manager_output.outputs == 0:
            event_list = prism.prism_logging.fire_console_event(
                setup_event_manager_output.event_to_fire, event_list, log_level='error'
            )
            return TaskRunReturnResult(
                event_list, True
            )

        # Warning events
        warning_events = setup_event_manager_output.outputs

        # Trigger header events
        triggers_to_exec = getattr(self, f"{trigger_type}_triggers")
        if len(triggers_to_exec) > 0:
            event_list = prism.prism_logging.fire_console_event(
                prism.prism_logging.TriggersHeaderEvent(),
                event_list,
            )

            # Warning to indicate we defaulted to project directory
            if self.defaulted_to_project_dir:
                event_list = prism.prism_logging.fire_console_event(
                    prism.prism_logging.TriggersPathNotDefined(),
                    event_list,
                    log_level='warn'
                )

            # Fire all other warnings encountered during setup
            for ev in warning_events:
                event_list = prism.prism_logging.fire_console_event(
                    ev,
                    event_list,
                    log_level='warn'
                )

        # Execute triggers
        cb_has_error = False
        for cb in triggers_to_exec:
            cb_event_manager = BaseEventManager(
                idx=None,
                total=None,
                name=f'{cb.name}',
                full_tb=full_tb,
                func=cb.execute_trigger
            )
            cb_event_manager_output = cb_event_manager.manage_events_during_run(
                event_list=event_list,
                run_context=run_context,
                fire_empty_line_events=False
            )
            event_list = cb_event_manager_output.event_list

            # Fire the error event. Don't return after the first error, because we want
            # to execute all triggers.
            if cb_event_manager_output.outputs == 0:
                cb_has_error = True
                event_list = prism.prism_logging.fire_empty_line_event(event_list)
                event_list = prism.prism_logging.fire_console_event(
                    cb_event_manager_output.event_to_fire, event_list, log_level='error'
                )
        return TaskRunReturnResult(
            event_list, cb_has_error
        )
