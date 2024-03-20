# Other imports
from pathlib import Path

import prism.decorators
import prism.target

# Prism infrastructure imports
import prism.task
from prism.runtime import CurrentRun


class Task01(prism.task.PrismTask):
    def done(self):
        return (Path(CurrentRun.ctx("OUTPUT")) / "task01.txt").is_file()

    # Run
    @prism.decorators.target(
        type=prism.target.Txt, loc=Path(CurrentRun.ctx("OUTPUT")) / "task01.txt"
    )
    def run(self):
        return "Hello from task 1!"
