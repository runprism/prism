from prism.runtime import CurrentRun
from prism.task import PrismTask


class Taske(PrismTask):
    def run(self):
        return (
            CurrentRun.ref("moduleA.Taska")
            + CurrentRun.ref("moduleC.Taskc")
            + CurrentRun.ref("moduleD.Taskd")
            + " This is task E."
        )  # noqa
