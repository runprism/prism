from prism.runtime import CurrentRun
from prism.task import PrismTask


class Taskd(PrismTask):
    def run(self):
        return (
            CurrentRun.ref("taskB.Taskb")
            + CurrentRun.ref("taskA.Taska")
            + CurrentRun.ref("taskC.Taskc")
            + " This is task D."
        )  # noqa
