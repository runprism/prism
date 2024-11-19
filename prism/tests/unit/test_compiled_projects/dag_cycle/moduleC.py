from prism.runtime import Ref
from prism.task import PrismTask


class Taskc(PrismTask):
    def run(self):
        return Ref("moduleA.Taska") + " This is task C."
