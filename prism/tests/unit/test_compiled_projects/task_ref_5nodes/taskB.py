from prism.task import PrismTask
from prism.runtime import CurrentRun


class Taskb(PrismTask):

    def run(self):
        return CurrentRun.ref("taskA.Taska") + " This is task B."
