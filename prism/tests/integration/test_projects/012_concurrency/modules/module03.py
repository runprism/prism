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
import pandas as pd


######################
## Class definition ##
######################

class Task03(prism.task.PrismTask):

    def get_txt_output(self, path):
        with open(path) as f:
            lines = f.read()
        f.close()
        return lines
    
    ## Run
    def run(self, tasks, hooks):
        """
        Execute task.

        args:
            tasks: used to reference output of other tasks --> tasks.ref('...')
            hooks: built-in Prism hooks. These include:
                - hooks.dbt_ref --> for getting dbt tasks as a pandas DataFrame
                - hooks.sql     --> for executing sql query using an adapter in profile YML
                - hooks.spark   --> for accessing SparkSession (if pyspark specified in profile YML)
        returns:
            task output
        """
        task1_times = pd.read_csv(tasks.ref('module01.py'))
        task2_times = pd.read_csv(tasks.ref('module02.py'))
        return 'Hello from task 3!'


# EOF