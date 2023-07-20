
# Standard library imports
import requests
import json

# Prism imports
import prism_project
from prism.decorators import task, target
from prism.target import JSON


# Task
@task(
    targets=[target(type=JSON, loc=prism_project.OUTPUT / 'astros.json')],
)
def extract(tasks, hooks):
    url = "http://api.open-notify.org/astros.json"
    resp = requests.get(url)
    return json.loads(resp.text)
