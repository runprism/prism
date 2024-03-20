import prism.task
from prism.runtime import CurrentRun


class Task06(prism.task.PrismTask):
    def run(self):
        return CurrentRun.ref("task05.Task05") + "This is task 06. "
