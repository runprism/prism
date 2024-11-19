# Standard library imports
import json

import requests

# Prism imports
from prism.decorators import target, task
from prism.runtime import Context
from prism.target import JSON


# Task
@task(
    task_id="extract_task",
    targets=[target(type=JSON, loc=Context("OUTPUT") / "todos.json")],
)
def extract():
    url = "https://jsonplaceholder.typicode.com/todos"
    resp = requests.get(url)
    return json.loads(resp.text)
