
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
import json
from pathlib import Path


####################
# Task definitions #
####################

@task()
def transform(tasks, hooks):
    data = tasks.ref("extract.extract")

    # Filter
    independent_countries = []
    for c in data:
        try:
            if c["independent"]:
                independent_countries.append(c)
        except KeyError:
            continue
    return sorted(independent_countries, key=lambda x: x["name"]["common"])


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
