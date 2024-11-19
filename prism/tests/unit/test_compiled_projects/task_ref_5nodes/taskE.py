from prism.runtime import Ref
from prism.task import PrismTask


class Taske(PrismTask):
    def run(self):
        return (
            Ref("taskA.Taska")
            + Ref("taskC.Taskc")
            + Ref("taskD.Taskd")
            + " This is task E."
        )  # noqa
