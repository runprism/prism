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

class ModelManifest:
    """
    Class used to store metadata on a parsed model
    """

    def __init__(self):
        self.manifest_dict: Dict[str, Any] = {"targets": [], "models": [], "refs": []}

    def add_model(self, model_name: Path):
        self.manifest_dict["models"].append(str(model_name))

    def add_ref(self, target: Path, source: Path):
        obj = {
            "target": str(target),
            "source": str(source)
        }
        self.manifest_dict["refs"].append(obj)

    def add_target(self, model_name: Path, loc: Union[str, List[str]]):
        obj = {
            "model_name": str(model_name),
            "target_locs": loc
        }
        self.manifest_dict["targets"].append(obj)


class Manifest:
    """
    Class used to store metadata on compiled prism project
    """

    def __init__(self, model_manifests: List[ModelManifest] = []):
        self.manifest_dict: Dict[str, Any] = {
            "targets": [], "prism_project": "", "models": [], "refs": []
        }
        self.model_manifests = model_manifests

        # Iterate through model manifests and add to manifest
        for mm in self.model_manifests:
            self.manifest_dict["targets"].extend(mm.manifest_dict["targets"])
            self.manifest_dict["models"].extend(mm.manifest_dict["models"])
            self.manifest_dict["refs"].extend(mm.manifest_dict["refs"])

    def add_prism_project(self, prism_project_data: str):
        self.manifest_dict["prism_project"] = prism_project_data

    def add_model(self, model_name: Path, model_data: str):
        obj = {
            "model_name": str(model_name),
            "model_data": model_data
        }
        self.manifest_dict["models"].append(obj)

    def add_ref(self, target: Path, source: Path):
        obj = {
            "target": str(target),
            "source": str(source)
        }
        self.manifest_dict["refs"].append(obj)

    def add_target(self, model_name: Path, loc: Union[str, List[str]]):
        obj = {
            "model_name": str(model_name),
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
