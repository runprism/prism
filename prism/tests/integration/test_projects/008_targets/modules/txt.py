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
from prism.target import target, Txt


######################
## Class definition ##
######################

class Module02(PrismTask):

    ## Run    
    @target(type=Txt, loc=f'{prism_project.OUTPUT}/target_txt.txt')
    def run(self, psm):
        string = "Hello, world!"
        return string


# EOF