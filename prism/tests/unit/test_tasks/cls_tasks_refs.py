from prism.runtime import Ref
from prism.task import PrismTask


class TasksRefs(PrismTask):
    def func_0(self):
        return Ref("func_0")

    def run(self):
        _ = Ref("hello")
        _ = Ref("world")
        return "hi"

    def func_1(self):
        return Ref("func_1")
