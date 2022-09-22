"""PRIVILEGED AND CONFIDENTIAL; FOR INTERNAL USE ONLY

In this script, we... 

--------------------------------------------------------------------------------
Table of Contents:
- Imports
- Class definition
    - Run
--------------------------------------------------------------------------------
"""

#############
## Imports ##
#############

import prism_project
from prism.task import PrismTask
from prism.target import PandasCsv
from prism.decorators import target
import pandas as pd


######################
## Class definition ##
######################

class Module03(PrismTask):

    ## Run    
    @target(type=PandasCsv, loc=f'{prism_project.OUTPUT}/target_csv.csv', index=False)
    def run(self, tasks, hooks):
        data = {
            'col1': ['col1_value1', 'col1_value2', 'col1_value3'],
            'col2': ['col2_value1', 'col2_value2', 'col2_value3'],
            'col3': ['col3_value1', 'col3_value2', 'col3_value3'],
            'col4': ['col4_value1', 'col4_value2', 'col4_value3'],
            'col5': ['col5_value1', 'col5_value2', 'col5_value3'],
            'col6': ['col6_value1', 'col6_value2', 'col6_value3']
        }
        df = pd.DataFrame(data)
        return df


# EOF