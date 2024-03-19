import prism.task
from prism.runtime import CurrentRun


class Task09(prism.task.PrismTask):

    def run(self):
        return (
            CurrentRun.ref("task05.Task05")
            + CurrentRun.ref("task08.Task08")
            + "This is task 09. "
        )  # noqa: E501
