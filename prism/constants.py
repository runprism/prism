"""
Prism constants.
"""

###########
# Imports #
###########

import os
from pathlib import Path
import sys


#############
# Constants #
#############

# Version number
VERSION = '0.1.6rc3'

# Root directory of project
ROOT_DIR = str(Path(os.path.dirname(__file__)).parent)

# Files to ignore when instantiating Prism project
IGNORE_FILES = ["__pycache__", '*checkpoint.ipynb', '.ipynb_checkpoints']

# Profile/adapter constants
VALID_PROFILE_KEYS = ["adapters"]
VALID_ADAPTERS = [
    "snowflake",
    "pyspark",
    "dbt",
    "bigquery",
    "redshift",
    "postgres"
]

# Context
CONTEXT = {'__name__': '__main__'}

# Internal names for task_manager and hooks
INTERNAL_TASK_MANAGER_VARNAME = '__PRISM_TASK_MANAGER__'
INTERNAL_HOOKS_VARNAME = '__PRISM_HOOKS__'

# Python version
PYTHON_VERSION = sys.version_info
