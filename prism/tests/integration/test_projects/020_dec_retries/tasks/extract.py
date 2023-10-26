
# Standard library imports
import requests
import json

# Prism imports
import prism_project
from prism.decorators import task, target
from prism.target import JSON


# Task
@task(
    targets=[target(type=JSON, loc=prism_project.OUTPUT / 'todos.json')],
)
def extract(tasks, hooks):
    url = "https://jsonplaceholder.typicode.com/todos"
    resp = requests.get(url)
    return json.loads(resp.text)
