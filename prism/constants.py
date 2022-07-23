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
VERSION = '0.1.0-alpha'

# Root directory of project
ROOT_DIR =  str(Path(os.path.dirname(__file__)).parent)

# Files to ignore when instantiating Prism project
IGNORE_FILES = ["__init__.py", "__pycache__", '*checkpoint.ipynb', '.ipynb_checkpoints', '.exists']

# Profile/adapter constants
VALID_PROFILE_KEYS = ["adapters"]
VALID_ADAPTERS = [ "snowflake", "pyspark", "dbt", "bigquery"]
VALID_SQL_ADAPTERS = [ "snowflake", "bigquery"]
VALID_CONNECTIONS = VALID_ADAPTERS

# UI
TERMINAL_WIDTH = 80

# Scope dictionary
GLOBALS_DICT = {'__name__': '__main__'}


# EOF