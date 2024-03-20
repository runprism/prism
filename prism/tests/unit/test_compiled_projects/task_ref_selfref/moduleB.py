from prism.runtime import CurrentRun
from prism.task import PrismTask


class Taskb(PrismTask):
    def run(self):
        return CurrentRun.ref("moduleB")
