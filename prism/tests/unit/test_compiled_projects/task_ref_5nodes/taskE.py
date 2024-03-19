from prism.task import PrismTask
from prism.runtime import CurrentRun


class Taske(PrismTask):

    def run(self):
        return (
            CurrentRun.ref("taskA.Taska")
            + CurrentRun.ref("taskC.Taskc")
            + CurrentRun.ref("taskD.Taskd")
            + " This is task E."
        )  # noqa
