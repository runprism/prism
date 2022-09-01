"""
In this script, we... 
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

class Module01(PrismTask):

    ## Run
    @target(type=Txt, loc=prism_project.OUTPUT / 'hello_world.txt')
    def run(self, mods, hooks):
        """
        Execute task.

        args:
            mods: object used to access the output of other tasks, e.g.,:
                mods.ref('some_other_task.py')
            hooks: hooks used to augment Prism functionality. These include:
                hooks.sql     --> for executing sql query using an adapter in profile.yml
                hooks.spark   --> for accessing SparkSession (if pyspark specified in profile.yml)
                hooks.dbt_ref --> for getting dbt models as a pandas DataFrame
        returns:
            task output
        """
        return "Hello, world!"


# EOF