from prism.runtime import Ref
from prism.task import PrismTask


class Func1(PrismTask):
    task_id = "func1"

    def run(self):
        Ref("func0")
        return "world"
