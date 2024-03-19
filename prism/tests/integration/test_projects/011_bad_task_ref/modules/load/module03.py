# Prism imports
import prism.task
import prism.target
import prism.decorators
from prism.runtime import CurrentRun


class Task03(prism.task.PrismTask):

    # Run
    def run(self):
        lines = CurrentRun.ref("extract/module02.Task02")
        return lines + "\n" + "Hello from task 3!"
