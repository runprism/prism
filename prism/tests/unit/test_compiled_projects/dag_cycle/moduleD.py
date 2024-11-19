from prism.runtime import Ref
from prism.task import PrismTask


class Taskd(PrismTask):
    def run(self):
        return (
            Ref("moduleB.Taskb")
            + Ref("moduleA.Taska")
            + Ref("moduleC.Taskc")
            + " This is task D."
        )  # noqa
