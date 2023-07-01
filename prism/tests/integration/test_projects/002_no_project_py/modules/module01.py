###########
# Imports #
###########

# Prism infrastructure imports
import prism.task
import prism.target
import prism.decorators


######################
## Class definition ##
######################

class Model01(prism.task.PrismTask):

    ## Run
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
        return "Hello, world!"


# EOF