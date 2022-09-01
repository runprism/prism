"""
Constants used throughout the prism architecture
"""

#############
## Imports ##
#############
import os
from pathlib import Path


###############
## Constants ##
###############

# Version number
VERSION = '0.1.6rc1'

# Root directory of project
ROOT_DIR =  str(Path(os.path.dirname(__file__)).parent)

# Files to ignore when instantiating Prism project
IGNORE_FILES = ["__init__.py", "__pycache__", '*checkpoint.ipynb', '.ipynb_checkpoints']

# Profile/adapter constants
VALID_PROFILE_KEYS = ["adapters"]
VALID_ADAPTERS = [ "snowflake", "pyspark", "dbt", "bigquery", "redshift"]
VALID_SQL_ADAPTERS = [ "snowflake", "bigquery", "redshift"]
VALID_CONNECTIONS = VALID_ADAPTERS

# UI
TERMINAL_WIDTH = 80

# Scope dictionary
GLOBALS_DICT = {'__name__': '__main__'}

# Internal names for mods and hooks
INTERNAL_MODS_VARNAME = '__PRISM_MODS__'
INTERNAL_HOOKS_VARNAME = '__PRISM_HOOKS__'


# EOF