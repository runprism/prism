import prism.task
from prism.runtime import CurrentRun


class Task04(prism.task.PrismTask):

    def run(self):
        return (
            CurrentRun.ref("task02.Task02")
            + CurrentRun.ref("task03.Task03")
            + "This is task 04. "
        )  # noqa: E501
