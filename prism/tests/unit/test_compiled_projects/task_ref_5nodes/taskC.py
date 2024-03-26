from prism.runtime import CurrentRun
from prism.task import PrismTask


class Taskc(PrismTask):
    def run(self):
        return CurrentRun.ref("taskA.Taska") + " This is task C."
