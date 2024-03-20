from pathlib import Path

import prism.decorators
import prism.target

# Prism imports
import prism.task
from prism.runtime import CurrentRun


class Task01(prism.task.PrismTask):
    # Run
    @prism.decorators.target(
        type=prism.target.Txt, loc=Path(CurrentRun.ctx("OUTPUT")) / "task01.txt"
    )
    def run(self):
        return "Hello from task 1!"
