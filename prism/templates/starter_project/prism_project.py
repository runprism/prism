"""
@name: ...
@author: ...
@version: ...
@description: ...

--------------------------------------------------------------------------------
Table of Contents
- Imports
- Thread count
- Profile name
- Logger
- Global variables / paths
"""

# Imports
import logging
from pathlib import Path


# sys.path config. This gives your tasks access to local modules / packages that exist
# outside of your project structure.
SYS_PATH_CONF = [
    Path(__file__).parent,
    # Add more paths here!
]


# Thread count: number of workers to use to execute tasks concurrently. If set to 1,
# then 1 task is run at a time.
THREADS = 1


# Profile directory and name
PROFILES_DIR = Path(__file__).parent  # location of `profiles.yml` file
PROFILE = None  # name of profile within `profiles.yml`


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
