import prism.task
from prism.runtime import CurrentRun


class Task02(prism.task.PrismTask):
    def run(self):
        return CurrentRun.ref("task01.Task01") + "This is task 02."
