from prism.runtime import Ref
from prism.task import PrismTask


class Taskd(PrismTask):
    def run(self):
        return (
            Ref("taskB.Taskb")
            + Ref("taskA.Taska")
            + Ref("taskC.Taskc")
            + " This is task D."
        )  # noqa
