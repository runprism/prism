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
from pathlib import Path
from typing import Any, Dict, List
import re


####################
# Class definition #
####################

class TaskManifest:
    """
    Class used to store metadata on a parsed task
    """

    def __init__(self):
        self.manifest_dict: Dict[str, Any] = {"targets": {}, "tasks": [], "refs": {}}

    def add_task(self, task_name: Path):
        self.manifest_dict["tasks"].append(str(task_name))

    def add_refs(self,
        target_module: Path,
        target_task: str,
        sources: List[str]
    ):
        target_module_no_py = re.sub(r'\.py$', '', str(target_module))
        if target_module_no_py not in self.manifest_dict["refs"].keys():
            self.manifest_dict["refs"][target_module_no_py] = {}
        self.manifest_dict["refs"][target_module_no_py][target_task] = sources

    def add_targets(self,
        module_relative_path: Path,
        task_name: str,
        locs: List[str]
    ):
        module_name_no_py = re.sub(r'\.py$', '', str(module_relative_path))
        if module_name_no_py not in self.manifest_dict["targets"].keys():
            self.manifest_dict["targets"][module_name_no_py] = {}
        self.manifest_dict["targets"][module_name_no_py][task_name] = locs


class Manifest:
    """
    Class used to store metadata on compiled prism project
    """

    def __init__(self, task_manifests: List[TaskManifest] = []):
        self.manifest_dict: Dict[str, Any] = {
            "targets": {}, "prism_project": "", "tasks": [], "refs": {}
        }
        self.task_manifests = task_manifests

        # Iterate through task manifests and add to manifest
        for mm in self.task_manifests:
            self.manifest_dict["targets"].update(mm.manifest_dict["targets"])
            self.manifest_dict["tasks"].extend(mm.manifest_dict["tasks"])
            self.manifest_dict["refs"].update(mm.manifest_dict["refs"])

    def add_prism_project(self, prism_project_data: str):
        self.manifest_dict["prism_project"] = prism_project_data

    def json_dump(self, path: Path):
        with open(path / 'manifest.json', 'w') as f:
            json.dump(self.manifest_dict, f, sort_keys=False)
        f.close()

    def json_load(self, path: Path):
        with open(path / 'manifest.json', 'r') as f:
            manifest = json.loads(f.read())
        f.close()
        return manifest
