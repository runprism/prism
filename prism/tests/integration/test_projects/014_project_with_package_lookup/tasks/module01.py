# From additional package lookup
from additional_package.utils import task1_return

import prism.decorators
import prism.target

# Prism imports
import prism.task
from prism.runtime import CurrentRun


class Task01(prism.task.PrismTask):
    # Run
    @prism.decorators.target(
        type=prism.target.Txt, loc=CurrentRun.ctx("OUTPUT") / "task01.txt"
    )
    def run(self):
        return task1_return()
