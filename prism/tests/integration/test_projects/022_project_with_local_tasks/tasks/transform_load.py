
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
from pathlib import Path


####################
# Task definitions #
####################

@task()
def transform(tasks, hooks):
    data = tasks.ref("extract.extract")

    # Filter to countries whose status is `independent`
    independent_countries = [c for c in data if c["independent"]]
    return independent_countries


@task(
    targets=[
        target(
            type=prism.target.JSON,
            loc=Path(prism_project.OUTPUT / 'independent_countries.json')
        )
    ]
)
def load(tasks, hooks):
    transformed_data = tasks.ref("transform", local=True)
    return transformed_data
