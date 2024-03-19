# Standard library imports

# Prism imports
from prism.target import Txt
from prism.decorators import task, target_iterator
from prism.runtime import CurrentRun


# Task
@task(
    retries=1,
    retry_delay_seconds=0,
    targets=[target_iterator(type=Txt, loc=CurrentRun.ctx("OUTPUT"))],
)
def load():
    data = CurrentRun.ref("extract_task")

    # Add an error for testing
    print(hi)  # noqa: F821

    # Names
    names = {}
    for ppl in data["people"]:

        # Formatted
        name = ppl["name"].lower().replace(" ", "_")
        names[f"{name}.txt"] = ppl["name"]

    # Return
    return names
