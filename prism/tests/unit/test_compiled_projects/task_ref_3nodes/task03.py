import prism.task
from prism.runtime import CurrentRun


class Task03(prism.task.PrismTask):

    def run(self):
        return CurrentRun.ref("task02.Task02") + "This is task 3."
