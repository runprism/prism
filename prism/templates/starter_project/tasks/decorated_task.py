from pathlib import Path

# Prism imports
import prism.target
from prism.decorators import (
    task,
    target,
)
from prism.runtime import CurrentRun


@task(
    task_id="example-decorated-task",
    targets=[
        target(
            type=prism.target.Txt,
            loc=Path(CurrentRun.ctx("OUTPUT")) / "hello_world.txt"
        )
    ],
)
def example_task():
    return "Hello, world!"
