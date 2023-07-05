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

# Prism imports
import prism.exceptions


####################
# Class definition #
####################

class ModelManifest:
    """
    Class used to store metadata on a parsed model
    """

    def __init__(self):
        self.manifest_dict: Dict[str, Any] = {"targets": {}, "models": [], "refs": {}}

    def add_model(self, model_name: Path):
        self.manifest_dict["models"].append(str(model_name))

    def add_refs(self,
        target_module: Path,
        target_model: str,
        sources: List[str]
    ):
        target_module_no_py = re.sub(r'\.py$', '', str(target_module))
        if target_module_no_py not in self.manifest_dict["refs"].keys():
            self.manifest_dict["refs"][target_module_no_py] = {}

        # If the model is already in the manifest, then raise an error, because we'd
        # be double-adding the refs
        if target_model in self.manifest_dict["refs"][target_module_no_py].keys():  # noqa: E501
            raise prism.exceptions.ParserException(
                message=f"manifest already contains refs for model `{target_module_no_py}.{target_model}`"  # noqa: E501
            )
        else:
            self.manifest_dict["refs"][target_module_no_py][target_model] = sources

    def add_targets(self,
        module_relative_path: Path,
        model_name: str,
        locs: List[str]
    ):
        module_name_no_py = re.sub(r'\.py$', '', str(module_relative_path))
        if module_name_no_py not in self.manifest_dict["targets"].keys():
            self.manifest_dict["targets"][module_name_no_py] = {}

        # If the model is already in the manifest, then raise an error, because we'd
        # be double-adding the targets
        if model_name in self.manifest_dict["targets"][module_name_no_py].keys():  # noqa: E501
            raise prism.exceptions.ParserException(
                message=f"manifest already contains targets for model `{module_name_no_py}.{model_name}`"  # noqa: E501
            )
        else:
            self.manifest_dict["targets"][module_name_no_py][model_name] = locs


class Manifest:
    """
    Class used to store metadata on compiled prism project
    """

    def __init__(self, model_manifests: List[ModelManifest] = []):
        self.manifest_dict: Dict[str, Any] = {
            "targets": {}, "prism_project": "", "models": [], "refs": {}
        }
        self.model_manifests = model_manifests

        # Iterate through model manifests and add to manifest
        for mm in self.model_manifests:
            self.manifest_dict["targets"].update(mm.manifest_dict["targets"])
            self.manifest_dict["models"].extend(mm.manifest_dict["models"])
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
