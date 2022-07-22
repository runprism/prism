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
from prism.task import PrismTask       # Not necessary; prism infrastructure automatically imported on the back-end
import prism.target as PrismTarget     # Not necessary; prism infrastructure automatically imported on the back-end


######################
## Class definition ##
######################

class FilterCustomers(PrismTask):

    ## Run    
    @PrismTask.target(type=PrismTarget.PandasCsv, loc=f'{prism_project.OUTPUT}/jaffle_shop_customers.csv', index=False)
    def run(self, psm):
        df = psm.dbt_ref('customers')
        df_new = df.iloc[:10]
        return df_new


# EOF