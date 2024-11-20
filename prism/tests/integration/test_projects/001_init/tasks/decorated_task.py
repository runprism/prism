from pathlib import Path

# Prism imports
import prism.target
from prism.decorators import target, task
from prism.runtime import Context


@task(
    task_id="example-decorated-task",
    targets=[
        target(
            type=prism.target.Txt,
            loc=Path(Context("OUTPUT", "output")).resolve() / "hello_world.txt",
        )
    ],
)
def example_task():
    return "Hello, world!"
