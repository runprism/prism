from prism.task import PrismTask
from prism.runtime import CurrentRun


class Taskc(PrismTask):

    def run(self):
        return CurrentRun.ref("taskA.Taska") + " This is task C."
