from prism.runtime import CurrentRun
from prism.task import PrismTask


class Taskc(PrismTask):
    def run(self):
        return CurrentRun.ref("moduleA.Taska") + " This is task C."
