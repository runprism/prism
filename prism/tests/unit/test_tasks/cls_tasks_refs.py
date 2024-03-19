from prism.task import PrismTask
from prism.runtime import CurrentRun


class TasksRefs(PrismTask):

    def func_0(self):
        return CurrentRun.ref("func_0")

    def run(self):
        _ = CurrentRun.ref("hello")
        _ = CurrentRun.ref("world")
        return "hi"

    def func_1(self):
        return CurrentRun.ref("func_1")
