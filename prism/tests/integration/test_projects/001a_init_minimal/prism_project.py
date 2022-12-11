"""
@name: 001a_init_minimal
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

###########
# Imports #
###########

import logging
from pathlib import Path


###################
# sys.path config #
###################

SYS_PATH_CONF = [
    Path(__file__).parent,
    # Add more paths here!
]


################
# Thread count #
################

# Number of workers to use to execute tasks concurrently. If set to 1, then 1 task is
# run at a time.
THREADS = 1


################
# Profile name #
################

# If connecting to a data warehouse (e.g., Snowflake), specify the profile you want to
# use. Profiles can be created with the prism connect command.
PROFILES_DIR = Path(__file__).parent
PROFILE = None


##########
# Logger #
##########

# The logger used to record events is called PRISM_LOGGER. Use this logger for your
# project
PRISM_LOGGER = logging.getLogger("PRISM_LOGGER")


############################
# Global variables / paths #
############################

# Specify global variables, parameters and paths to be used in the analysis. Capitalize
# all names.
VAR_1 = {'a': 'b'}
VAR_2 = 200
VAR_3 = '2015-01-01'

# Paths
WKDIR = Path(__file__).parent
DATA = WKDIR / 'data'
OUTPUT = WKDIR / 'output'
