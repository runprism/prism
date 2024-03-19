from prism.task import PrismTask
from prism.runtime import CurrentRun


class Taskb(PrismTask):

    def run(self):
        return (
            CurrentRun.ref("moduleA.Taska")
            + CurrentRun.ref("moduleE.Taske")
            + " This is task B."
        )  # noqa: E501
