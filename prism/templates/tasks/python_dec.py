
TEMPLATE = """
###########
# Imports #
###########

# Prism infrastructure imports
from prism.decorators import task, target

# Prism project imports
import prism_project


###################
# Task definition #
###################

@task(
    retries=0,
    retry_delay_seconds=0,
    targets=[
        target(...)
    ]
)
def {{ task_name }}(tasks, hooks):
    \"\"\"
    Execute task.
    \"\"\"
    #TODO: implement

"""
