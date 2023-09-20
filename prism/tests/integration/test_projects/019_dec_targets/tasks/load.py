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
    names = {}
    for ppl in data["people"]:

        # Formatted
        name = ppl["name"].lower().replace(" ", "_")
        names[f"{name}.txt"] = ppl["name"]

    # Return
    return names
