# Prism imports
import prism.decorators
import prism.target
import prism.task
from prism.runtime import Ref


class Task03(prism.task.PrismTask):
    # Run
    def run(self):
        lines = Ref("module02.Task02")
        return lines + "\n" + "Hello from task 3!"
