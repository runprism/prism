
# Standard library imports
import requests
import json

# Prism imports
import prism_project
from prism.decorators import task, target
from prism.target import JSON, Txt


# Task
@task(
    targets=[
        target(type=JSON, loc=prism_project.OUTPUT / 'astros.json'),
        target(type=Txt, loc=prism_project.OUTPUT / 'second_target.txt')
    ],
)
def extract(tasks, hooks):
    url = "http://api.open-notify.org/astros.json"
    resp = requests.get(url)
    json_dict = json.loads(resp.text)
    second_target_str = "second target"

    return json_dict, second_target_str
