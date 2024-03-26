# Standard library imports
import requests
import json

# Prism imports
from prism.decorators import task, target
from prism.target import JSON
from prism.runtime import CurrentRun


# Task
@task(
    task_id="extract_task",
    targets=[target(type=JSON, loc=CurrentRun.ctx("OUTPUT") / "todos.json")],
)
def extract():
    url = "https://jsonplaceholder.typicode.com/todos"
    resp = requests.get(url)
    return json.loads(resp.text)
