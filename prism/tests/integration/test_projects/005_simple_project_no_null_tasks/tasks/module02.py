
###########
# Imports #
###########

# Prism infrastructure imports
import prism.task
import prism.target
import prism.decorators

# Prism project imports
import prism_project


####################
# Class definition #
####################

class Task02(prism.task.PrismTask):

    # Run
    @prism.decorators.target(
        type=prism.target.Txt,
        loc=prism_project.OUTPUT / 'task02.txt'
    )
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
        # Use the module name and the task name to get the task output
        lines = tasks.ref('module01.Task01')
        return lines + "\n" + "Hello from task 2!"
