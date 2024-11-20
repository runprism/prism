from pathlib import Path

import prism.decorators
import prism.target

# Prism imports
import prism.task
from prism.runtime import Context


class ExampleTask(prism.task.PrismTask):
    task_id = "example-class-task"

    # Run
    @prism.decorators.target(
        type=prism.target.Txt,
        loc=Path(Context("OUTPUT", "output")).resolve() / "hello_world.txt",
    )
    def run(self):
        return "Hello, world!"
