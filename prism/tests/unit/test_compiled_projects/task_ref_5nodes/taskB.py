from prism.runtime import Ref
from prism.task import PrismTask


class Taskb(PrismTask):
    def run(self):
        return Ref("taskA.Taska") + " This is task B."
