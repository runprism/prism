"""
Prism constants.
"""

###########
# Imports #
###########

import os
from pathlib import Path
import sys
import builtins


#############
# Constants #
#############

# Version number
VERSION = '0.1.7rc2'

# Root directory of project
ROOT_DIR = str(Path(os.path.dirname(__file__)).parent)

# Files to ignore when instantiating Prism project
IGNORE_FILES = ["__pycache__", '*checkpoint.ipynb', '.ipynb_checkpoints']

# Profile/adapter constants
VALID_PROFILE_KEYS = ["adapters"]
VALID_ADAPTERS = [
    "bigquery",
    "dbt",
    "postgres",
    "pyspark",
    "redshift",
    "snowflake",
    "trino",
]

# Context
CONTEXT = {
    '__builtins__': builtins,
    '__name__': '__main__'
}

# Internal names for task_manager and hooks
INTERNAL_TASK_MANAGER_VARNAME = '__PRISM_TASK_MANAGER__'
INTERNAL_HOOKS_VARNAME = '__PRISM_HOOKS__'

# Python version
PYTHON_VERSION = sys.version_info

# Trigger types
VALID_TRIGGER_TYPES = ["function", "prism_project"]
