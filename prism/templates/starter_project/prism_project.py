"""
@name: ...
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

import os
import sys
import pandas as pd
import numpy as np


################
# Profile name #
################

# If connecting to a data warehouse (e.g., Snowflake), specify the profile you
# want to use. Profiles can be created with the prism connect command.
profile = None


####################################
# Variables, parameters, and paths #
####################################

# Specify global variables, parameters and paths to be used in the analysis. 
# Capitalize all names.
VAR_1 = {'a': 'b'}
VAR_2 = 200
VAR_3 = '2015-01-01'

# Paths
WKDIR = os.path.dirname(__file__)
DATA = os.path.join(WKDIR, 'data')
OUTPUT = os.path.join(WKDIR, 'output')


# EOF