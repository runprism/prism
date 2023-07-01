###########
# Imports #
###########

# Prism infrastructure imports
import prism.task
import prism.target
import prism.decorators

# Prism project imports
import prism_project

# Other imports
from pathlib import Path


######################
## Class definition ##
######################

class Model02(prism.task.PrismTask):
    
    ## Run
    @prism.decorators.target(type=prism.target.Txt, loc=Path(prism_project.OUTPUT) / 'model02.txt')
    def run(self, tasks, hooks):
        """
        Execute task.

        args:
            tasks: used to reference output of other tasks --> tasks.ref('...')
            hooks: built-in Prism hooks. These include:
                - hooks.dbt_ref --> for getting dbt models as a pandas DataFrame
                - hooks.sql     --> for executing sql query using an adapter in profile YML
                - hooks.spark   --> for accessing SparkSession (if pyspark specified in profile YML)
        returns:
            task output
        """
        with open(tasks.ref('model01'), 'r') as f:
            lines = f.read()
        f.close()
        return lines + "\n" + "Hello from model 2!"


# EOF