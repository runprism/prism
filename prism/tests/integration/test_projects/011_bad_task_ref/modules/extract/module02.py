# Prism imports
import prism.task
import prism.target
import prism.decorators
from prism.runtime import CurrentRun


class Task02(prism.task.PrismTask):

    # Run
    @prism.decorators.target(
        type=prism.target.Txt, loc=CurrentRun.ctx("OUTPUT") / "task02.txt"
    )
    def run(self):
        lines = CurrentRun.ref("extract/this_is_an_error")
        return lines + "\n" + "Hello from task 2!"
