# Standard library imports

# Prism imports
from prism.decorators import target_iterator, task
from prism.runtime import CurrentRun
from prism.target import Txt


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
