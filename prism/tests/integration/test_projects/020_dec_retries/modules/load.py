
# Standard library imports
import json

# Prism imports
import prism_project
from prism.target import Txt
from prism.decorators import task, target_iterator


# Task
@task(
    retries=1,
    retry_delay_seconds=0,
    targets=[target_iterator(type=Txt, loc=prism_project.OUTPUT)]
)
def load(tasks, hooks):
    extract_path = tasks.ref("extract.py")
    with open(extract_path, 'r') as f:
        data_str = f.read()
    data = json.loads(data_str)

    # Add an error for testing
    print(hi)

    # Names
    names = {}
    for ppl in data["people"]:

        # Formatted
        name = ppl["name"].lower().replace(" ", "_")
        names[f"{name}.txt"] = ppl["name"]

    # Return
    return names
