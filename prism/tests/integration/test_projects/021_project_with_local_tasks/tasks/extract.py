
###########
# Imports #
###########

# Prism infrastructure imports
from prism.decorators import (
    task,
    target
)
import prism.target
import prism_project

# Other imports
from pathlib import Path
import requests
import json


###################
# Task definition #
###################

@task(
    targets=[
        target(
            type=prism.target.JSON,
            loc=Path(prism_project.OUTPUT / 'all_countries.json')
        )
    ]
)
def extract(tasks, hooks):
    api_url = "https://restcountries.com/v3.1/all"
    resp = requests.get(api_url, verify=False)
    data = json.loads(resp.text)
    return data
