from prism.runtime import Ref
from prism.task import PrismTask


class Func0(PrismTask):
    task_id = "func0"

    def run(self):
        Ref("hello")
        Ref("world")
        return "world"
