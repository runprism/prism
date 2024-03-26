from pathlib import Path

# Prism imports
import prism.target
from prism.decorators import target, task
from prism.runtime import CurrentRun


@task(
    task_id="example-decorated-task",
    targets=[
        target(
            type=prism.target.Txt,
            loc=Path(CurrentRun.ctx("OUTPUT", "output")).resolve() / "hello_world.txt",
        )
    ],
)
def example_task():
    return "Hello, world!"
