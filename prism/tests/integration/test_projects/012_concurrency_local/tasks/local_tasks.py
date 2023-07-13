
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

# Other imports
import time
import pandas as pd


####################
# Class definition #
####################

@task(
    targets=[
        target(
            type=prism.target.PandasCsv,
            loc=prism_project.OUTPUT / 'local_task1.csv', index=False
        )
    ]
)
def local_task1(tasks, hooks):
    start_time = time.time()
    time.sleep(15)
    end_time = time.time()
    time_df = pd.DataFrame({
        'start_time': [start_time],
        'end_time': [end_time]
    })
    return time_df


@task(
    targets=[
        target(
            type=prism.target.PandasCsv,
            loc=prism_project.OUTPUT / 'local_task2.csv', index=False
        )
    ]
)
def local_task2(tasks, hooks):
    start_time = time.time()
    time.sleep(5)
    end_time = time.time()
    time_df = pd.DataFrame({
        'start_time': [start_time],
        'end_time': [end_time]
    })
    return time_df


@task()
def local_task3(tasks, hooks):
    _ = tasks.ref("local_task1", local=True)
    _ = tasks.ref("local_task2", local=True)
    return "This is a third local task"
