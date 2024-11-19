from prism.runtime import Ref
from prism.task import PrismTask


class Taskb(PrismTask):
    def run(self):
        return Ref("moduleA.Taska") + Ref("moduleE.Taske") + " This is task B."  # noqa: E501
