# Prism imports
import prism_project
from prism.target import Txt
from prism.decorators import task, target_iterator


# Task
@task(
    targets=[target_iterator(type=Txt, loc=prism_project.OUTPUT)]
)
def load(tasks, hooks):
    data, _ = tasks.ref("extract.py")

    # Names
    todos = {}
    for todo in data:
        todos[f"todo_{todo['id']}.txt"] = todo['title']

    # Return
    return todos
