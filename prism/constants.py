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
VERSION = '0.1.8rc2'

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

# Task types
VALID_TASK_TYPES = [
    "python",
    "pyspark",
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
VALID_TRIGGER_TYPES = ["function"]

# Default image to use for Docker agent
DEFAULT_DOCKER_IMAGE = "python:3.10.8-slim-bullseye"

# Docker image build template
DOCKER_IMAGE_BUILD_TEMPLATE = """FROM {base_image}

# Copy requirements first and install. This layer is rate limiting, so we want to cache
# it separately from copying the entire project directory.
COPY {requirements_txt_path} ./{requirements_txt_path}
RUN pip install --upgrade pip && pip install -r ./{requirements_txt_path}

# Copy rest of project
COPY {project_dir} ./{project_dir}
{other_copy_commands}
WORKDIR ./{project_dir}

# Environment variables
{env}
"""
