import prism.task
import prism.target
import prism.decorators
from prism.runtime import CurrentRun


class Task02(prism.task.PrismTask):

    # Run
    @prism.decorators.target(
        type=prism.target.Txt, loc=CurrentRun.ctx("OUTPUT") / "task01.txt"
    )
    def run(self):
        lines = CurrentRun.ref("module01.Task01")
        return lines[-5:]
