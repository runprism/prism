"""
Prism Manifest class

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Standard library imports
import json
import re
from pathlib import Path
from typing import Any, Dict, List

####################
# Class definition #
####################


class TaskManifest:
    """
    Class used to store metadata on a parsed task
    """

    def __init__(self):
        self.manifest_dict: Dict[str, Any] = {"targets": {}, "tasks": {}, "refs": {}}

    def update_tasks_dir_key(
        self,
        key: str,
        level: Dict[str, Any] = {},
    ):
        if key not in level.keys():
            level[key] = {}
        return level[key]

    def add_task(self, task_module: Path, task_name: str):
        """
        We want the `tasks` key in our manifest to be structured as follows
            "tasks": {
                "<module_name>": [
                    "task_name1",
                    "task_name2"
                ],
                "<dir_name>/" {
                    "<nested_module_name1>": [
                        "nested_task_name3",
                        "nested_task_name3"
                    ]
                }
                ...
            }
        """
        task_module_no_py = re.sub(r"\.py$", "", str(task_module))

        # Determine if the task exists in a directory
        flag_in_dir = False
        task_module_no_py_split = task_module_no_py.split("/")
        if len(task_module_no_py_split) > 1:
            flag_in_dir = True

        # If the task lives in a module, then the module name should be the key
        if not flag_in_dir:
            if task_module_no_py in self.manifest_dict["tasks"].keys():
                self.manifest_dict["tasks"][task_module_no_py].append(task_name)
            else:
                self.manifest_dict["tasks"][task_module_no_py] = [task_name]

        # If task lives in a nested directory, then the directory name should be the
        # first key.
        else:
            # Create necessary nested directory keys
            base_level = self.manifest_dict["tasks"]
            for _k in task_module_no_py_split[:-1]:
                base_level = self.update_tasks_dir_key(f"{_k}/", base_level)

            # Update the module / task name
            if task_module_no_py_split[-1] in base_level.keys():
                base_level[task_module_no_py_split[-1]].append(task_name)
            else:
                base_level[task_module_no_py_split[-1]] = [task_name]

    def add_refs(self, target_module: Path, target_task: str, sources: List[str]):
        target_module_no_py = re.sub(r"\.py$", "", str(target_module))
        if target_module_no_py not in self.manifest_dict["refs"].keys():
            self.manifest_dict["refs"][target_module_no_py] = {}
        self.manifest_dict["refs"][target_module_no_py][target_task] = sources

    def add_targets(self, module_relative_path: Path, task_name: str, locs: List[str]):
        module_name_no_py = re.sub(r"\.py$", "", str(module_relative_path))
        if module_name_no_py not in self.manifest_dict["targets"].keys():
            self.manifest_dict["targets"][module_name_no_py] = {}
        self.manifest_dict["targets"][module_name_no_py][task_name] = locs


class Manifest:
    """
    Class used to store metadata on compiled prism project
    """

    def __init__(self, task_manifests: List[TaskManifest] = []):
        self.manifest_dict: Dict[str, Any] = {
            "targets": {},
            "prism_project": "",
            "tasks": {},
            "refs": {},
        }
        self.task_manifests = task_manifests

        # Iterate through task manifests and add to manifest
        for mm in self.task_manifests:
            self.manifest_dict["targets"].update(mm.manifest_dict["targets"])
            self.update(self.manifest_dict["tasks"], mm.manifest_dict["tasks"])
            self.manifest_dict["refs"].update(mm.manifest_dict["refs"])

    def update(
        self,
        manifest_dict: Dict[str, Any],
        task_manifest_dict: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Recursive function to update `manifest_dict` with the contents of
        `task_manifest_dict`. We need a recursive function, because the `tasks` key
        within the manifest.json can have a bunch of nested dictionaries.

        args:
            manifest_dict: manifest dictionary
            task_manifest_dict: task manifest dictionary
        returns:
            updated manifest_dict
        """
        # Iterate through the task manifest's contents. Note that they should only have
        # one key within `tasks`.
        for k, v in task_manifest_dict.items():
            if k not in manifest_dict.keys():
                manifest_dict[k] = v
            elif isinstance(manifest_dict[k], list):
                for _item in v:
                    if _item not in manifest_dict[k]:
                        manifest_dict[k].append(_item)

            # If the value is a dictionary and the manifest already has this dictionary,
            # then we'll need to recursively update the manifest's dictionary.
            elif isinstance(manifest_dict[k], dict):
                self.update(manifest_dict[k], v)
        return manifest_dict

    def add_prism_project(self, prism_project_data: str):
        self.manifest_dict["prism_project"] = prism_project_data

    def json_dump(self, path: Path):
        with open(path / "manifest.json", "w") as f:
            json.dump(self.manifest_dict, f, sort_keys=False)
        f.close()

    def json_load(self, path: Path):
        with open(path / "manifest.json", "r") as f:
            manifest = json.loads(f.read())
        f.close()
        return manifest
