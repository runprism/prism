from prism.runtime import Ref
from prism.task import PrismTask


class Taske(PrismTask):
    def run(self):
        return (
            Ref("moduleA.Taska")
            + Ref("moduleC.Taskc")
            + Ref("moduleD.Taskd")
            + " This is task E."
        )  # noqa
