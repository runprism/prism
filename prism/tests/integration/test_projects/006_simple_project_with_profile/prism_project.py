"""
@name: 006_simple_project_with_profile
@author: ...
@version: ...
@description: ...

--------------------------------------------------------------------------------
Table of Contents
- Imports
- Profile
- Variables / parameters
- Paths
"""

###########
# Imports #
###########

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

# Number of workers to use to execute tasks concurrently. If set to 1,
# then 1 task is run at a time.
THREADS = 1


################
# Profile name #
################

# If connecting to a data warehouse (e.g., Snowflake), specify the profile you
# want to use. Profiles can be created with the prism connect command.
PROFILE = None


####################################
# Variables, parameters, and paths #
####################################

# Specify global variables, parameters and paths to be used in the analysis. 
# Capitalize all names.
VAR_1 = {'a': 'b'}
VAR_2 = 200
VAR_3 = '2015-01-01'

# Paths
WKDIR = Path(__file__).parent
DATA = WKDIR / 'data'
OUTPUT = WKDIR / 'output'
