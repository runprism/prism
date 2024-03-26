import prism.task
from prism.runtime import CurrentRun


class Task13(prism.task.PrismTask):
    def run(self):
        return CurrentRun.ref("task10.Task10") + "This is task 13. "
