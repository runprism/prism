"""
Prism Manifest class

Table of Contents
- Imports
- Class definition
"""

#############
## Imports ##
#############

# Standard library imports
import json
import yaml
from pathlib import Path
from typing import Any, Dict


######################
## Class definition ##
######################

class Manifest:
    """
    Class used to store metadata on compiled prism project
    """

    def __init__(self):
        self.manifest_dict: Dict[str, Any] = {"targets": [], "modules": [], "refs": []}

    def add_module(self, module_name: Path, module_data: str):
        obj = {
            "module_name": str(module_name),
            "module_data": module_data
        }
        self.manifest_dict["modules"].append(obj)


    def add_ref(self, source: Path, target: Path):
        obj = {
            "source": str(source),
            "target": str(target)
        }
        self.manifest_dict["refs"].append(obj)


    def add_target(self, module_name: Path, loc: str):
        obj = {
            "module_name": str(module_name),
            "target_loc": loc
        }
        self.manifest_dict["targets"].append(obj)


    def add_element(self, elem: str, elem_dict: Dict[str, Any]):
        self.manifest_dict['manifest'][elem] = elem_dict


    def json_dump(self, path: Path):
        with open(path / 'manifest.json', 'w') as f:
            json.dump(self.manifest_dict, f, sort_keys=False)
        f.close()

        
    def yaml_dump(self, path: Path):
        with open(path / 'manifest.yml', 'w') as f:
            yaml.safe_dump(self.manifest_dict, f, sort_keys=False)
        f.close()


# EOF