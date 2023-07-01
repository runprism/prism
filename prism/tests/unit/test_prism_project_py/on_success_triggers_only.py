"""
Prism project
"""

# Imports
import os
import logging
from pathlib import Path


# sys.path config. This gives your tasks access to local models / packages that exist
# outside of your project structure.
SYS_PATH_CONF = [
    Path(__file__).parent,
    Path(__file__).parent.parent,
]


# Thread count: number of workers to use to execute tasks concurrently. If set to 1,
# then 1 task is run at a time.
THREADS = os.cpu_count()


# Profile directory and name
PROFILE_YML_PATH = Path(__file__).parent / 'profile.yml'
PROFILE = 'hi'  # name of profile within `profiles.yml`


# Logger
PRISM_LOGGER = logging.getLogger("PRISM_LOGGER")


# Other variables / parameters. Make sure to capitalize all of these!
VAR_1 = {'a': 'b'}
VAR_2 = 200
VAR_3 = '2015-01-01'

# Paths
WKDIR = Path(__file__).parent
DATA = WKDIR / 'data'
OUTPUT = WKDIR / 'output'


# Triggers
TRIGGERS_YML_PATH = Path(__file__).parent / 'triggers.yml'  # location of `triggers.yml` file
TRIGGERS = {
    'on_success': ["test_fn"],
}
