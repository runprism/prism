# Prism imports
import prism.decorators
import prism.target
import prism.task
from prism.runtime import CurrentRun


class Task04(prism.task.PrismTask):
    # Run
    def run(self):
        return CurrentRun.ref("module03.Task03") + "\n" + "Hello from task 4!"
