from prism.task import PrismTask
from prism.runtime import CurrentRun


class Taskd(PrismTask):

    def run(self):
        return (
            CurrentRun.ref("moduleB.Taskb")
            + CurrentRun.ref("moduleA.Taska")
            + CurrentRun.ref("moduleC.Taskc")
            + " This is task D."
        )  # noqa
