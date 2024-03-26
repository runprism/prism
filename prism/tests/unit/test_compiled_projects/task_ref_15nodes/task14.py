import prism.task
from prism.runtime import CurrentRun


class Task14(prism.task.PrismTask):
    def run(self):
        return CurrentRun.ref("task11.Task11") + "This is task 14. "
