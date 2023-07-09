###########
# Imports #
###########

# Prism infrastructure imports
import prism.task
import prism.target
import prism.decorators


####################
# Class definition #
####################

class Task03(prism.task.PrismTask):

    # Run
    def run(self, tasks, hooks):
        """
        Execute task.

        args:
            tasks: used to reference output of other tasks --> tasks.ref('...')
            hooks: built-in Prism hooks. These include:
            - hooks.dbt_ref --> for getting dbt tasks as a pandas DataFrame
            - hooks.sql     --> for executing sql query using an adapter in profile YML
            - hooks.spark   --> for accessing SparkSession
        returns:
            task output
        """
        with open(tasks.ref('extract/module02.py')) as f:
            lines = f.read()
        return lines + '\n' + 'Hello from task 3!'
