from pathlib import Path

# Prism infrastructure imports
import prism.task
import prism.target
import prism.decorators
from prism.runtime import CurrentRun


class Task02(prism.task.PrismTask):

    # Run
    @prism.decorators.target(
        type=prism.target.Txt, loc=Path(CurrentRun.ctx("OUTPUT")) / "task02.txt"
    )
    def run(self):
        lines = CurrentRun.ref("task01.Task01")
        return lines + "\n" + "Hello from task 2!"
