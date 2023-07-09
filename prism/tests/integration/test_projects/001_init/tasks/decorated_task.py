"""
In this script, we...
"""

###########
# Imports #
###########

# Prism infrastructure imports
import prism.target
from prism.decorators import (
    task,
    target,
)

# Prism project imports
import prism_project


###################
# Task definition #
###################

@task(
    targets=[
        target(type=prism.target.Txt, loc=prism_project.OUTPUT / 'hello_world.txt')
    ]
)
def example_task(tasks, hooks):
    """
    Execute task.

    args:
        tasks: used to reference output of other tasks --> tasks.ref('...')
        hooks: hooks used to augment Prism functionality. These include:
            hooks.sql     --> for executing sql query using an adapter in profile YML
            hooks.spark   --> for accessing SparkSession (if pyspark specified in profile YML)
            hooks.dbt_ref --> for getting dbt models as a pandas DataFrame
    returns:
        task output
    """
    return "Hello, world!"
