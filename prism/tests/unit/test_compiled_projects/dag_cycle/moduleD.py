from prism.runtime import CurrentRun
from prism.task import PrismTask


class Taskd(PrismTask):
    def run(self):
        return (
            CurrentRun.ref("moduleB.Taskb")
            + CurrentRun.ref("moduleA.Taska")
            + CurrentRun.ref("moduleC.Taskc")
            + " This is task D."
        )  # noqa
