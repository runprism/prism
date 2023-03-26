"""
Prism project
"""

# Imports
from prism.admin import generate_run_id, generate_run_slug
import logging
from pathlib import Path


# Project metadata
NAME = "Docker agent test"
AUTHOR = ""
VERSION = ""
DESCRIPTION = """
"""

# Admin
RUN_ID = generate_run_id()
SLUG = generate_run_slug()


# sys.path config. This gives your tasks access to local modules / packages that exist
# outside of your project structure.
SYS_PATH_CONF = [
    Path(__file__).parent,
    Path(__file__).parent.parent / 'test_trigger_yml'
]


# Thread count: number of workers to use to execute tasks concurrently. If set to 1,
# then 1 task is run at a time.
THREADS = 1


# Profile directory and name
PROFILES_DIR = Path(__file__).parent / 'paths_test'
PROFILE = "profile_agent_test"


# Logger
PRISM_LOGGER = logging.getLogger("PRISM_LOGGER")


# Triggers
TRIGGERS_DIR = Path(__file__).parent.parent / 'test_trigger_yml'
TRIGGERS = {
    'on_success': ["test_fn.test_agent_trigger"],
    'on_failure': [],
}


# Other variables / parameters. Make sure to capitalize all of these!
VAR_1 = {'a': 'b'}
VAR_2 = 200
VAR_3 = '2015-01-01'

# Paths
WKDIR = Path(__file__).parent
DATA = WKDIR / 'data'
OUTPUT = WKDIR / 'output'
