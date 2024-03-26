"""
Prism constants.
"""

# Imports
import os
from pathlib import Path
import sys


# Version number
VERSION = "0.3.0"


# Root directory of project
ROOT_DIR = str(Path(os.path.dirname(__file__)).parent)


# Files to ignore when instantiating Prism project
IGNORE_FILES = ["__pycache__", "*checkpoint.ipynb", ".ipynb_checkpoints"]


# Python version
PYTHON_VERSION = sys.version_info


# Internal folder for stuff created by Prism
INTERNAL_FOLDER = Path(os.path.expanduser("~/.prism"))
if not INTERNAL_FOLDER.is_dir():
    INTERNAL_FOLDER.mkdir(parents=True)
