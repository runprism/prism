
###########
# Imports #
###########

# Prism infrastructure imports
import prism.target
from prism.decorators import (
    task,
    target
)
import prism_project


####################
# Class definition #
####################

@task(
    targets=[
        target(
            type=prism.target.Txt,
            loc=prism_project.OUTPUT / 'non_local_task.txt'
        )
    ]
)
def non_local_task(tasks, hooks):
    """
    Use this task to confirm that when we run nodes upstream `local_task3`, this task
    isn't executed
    """
    txt = tasks.ref("local_tasks.local_task3")
    return txt
