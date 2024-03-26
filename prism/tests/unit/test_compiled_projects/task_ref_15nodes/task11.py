import prism.task
from prism.runtime import CurrentRun


class Task11(prism.task.PrismTask):
    def run(self):
        return (
            CurrentRun.ref("task07.Task07a")
            + CurrentRun.ref("task10.Task10")
            + "This is task 11."
        )  # noqa: E501
