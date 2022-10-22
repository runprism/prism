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


############################
# Profile name / directory #
############################

# If connecting to a data warehouse (e.g., Snowflake), specify the profile you
# want to use. Profiles can be created with the prism connect command.
PROFILE = "this_is_a_test!!!"


####################################
# Variables, parameters, and paths #
####################################

# Specify global variables, parameters and paths to be used in the analysis. Capitalize all names.
VAR_1 = {'a': 'b'}
VAR_2 = 200
VAR_3 = '2015-01-01'

# Paths
INPUT = '~/Desktop'
OUTPUT = '~/Documents'


# EOF