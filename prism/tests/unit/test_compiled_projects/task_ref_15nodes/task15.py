import prism.task
from prism.runtime import CurrentRun


class Task15(prism.task.PrismTask):

    def run(self):
        return CurrentRun.ref("task11.Task11") + "This is task 15. "
