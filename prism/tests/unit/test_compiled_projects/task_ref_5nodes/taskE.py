from prism.runtime import CurrentRun
from prism.task import PrismTask


class Taske(PrismTask):
    def run(self):
        return (
            CurrentRun.ref("taskA.Taska")
            + CurrentRun.ref("taskC.Taskc")
            + CurrentRun.ref("taskD.Taskd")
            + " This is task E."
        )  # noqa
