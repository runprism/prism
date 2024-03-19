from prism.task import PrismTask
from prism.runtime import CurrentRun


class Taske(PrismTask):

    def run(self):
        return (
            CurrentRun.ref("moduleA.Taska")
            + CurrentRun.ref("moduleC.Taskc")
            + CurrentRun.ref("moduleD.Taskd")
            + " This is task E."
        )  # noqa
