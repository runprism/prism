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
from typing import Any, Dict, List, Union


####################
# Class definition #
####################

class ModuleManifest:
    """
    Class used to store metadata on a parsed module
    """

    def __init__(self):
        self.manifest_dict: Dict[str, Any] = {"targets": [], "modules": [], "refs": []}

    def add_module(self, module_name: Path):
        self.manifest_dict["modules"].append(str(module_name))

    def add_ref(self, target: Path, source: Path):
        obj = {
            "target": str(target),
            "source": str(source)
        }
        self.manifest_dict["refs"].append(obj)

    def add_target(self, module_name: Path, loc: Union[str, List[str]]):
        obj = {
            "module_name": str(module_name),
            "target_locs": loc
        }
        self.manifest_dict["targets"].append(obj)


class Manifest:
    """
    Class used to store metadata on compiled prism project
    """

    def __init__(self, module_manifests: List[ModuleManifest] = []):
        self.manifest_dict: Dict[str, Any] = {"targets": [], "prism_project": "", "modules": [], "refs": []}
        self.module_manifests = module_manifests

        # Iterate through module manifests and add to manifest
        for mm in self.module_manifests:
            self.manifest_dict["targets"].extend(mm.manifest_dict["targets"])
            self.manifest_dict["modules"].extend(mm.manifest_dict["modules"])
            self.manifest_dict["refs"].extend(mm.manifest_dict["refs"])

    def add_prism_project(self, prism_project_data: str):
        self.manifest_dict["prism_project"] = prism_project_data

    def add_module(self, module_name: Path, module_data: str):
        obj = {
            "module_name": str(module_name),
            "module_data": module_data
        }
        self.manifest_dict["modules"].append(obj)

    def add_ref(self, target: Path, source: Path):
        obj = {
            "target": str(target),
            "source": str(source)
        }
        self.manifest_dict["refs"].append(obj)

    def add_target(self, module_name: Path, loc: Union[str, List[str]]):
        obj = {
            "module_name": str(module_name),
            "target_locs": loc
        }
        self.manifest_dict["targets"].append(obj)

    def json_dump(self, path: Path):
        with open(path / 'manifest.json', 'w') as f:
            json.dump(self.manifest_dict, f, sort_keys=False)
        f.close()

    def json_load(self, path: Path):
        with open(path / 'manifest.json', 'r') as f:
            manifest = json.loads(f.read())
        f.close()
        return manifest
