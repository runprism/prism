from prism.task import PrismTask
from prism.runtime import CurrentRun


class Func0(PrismTask):
    task_id = "func0"

    def run(self):
        CurrentRun.ref("hello")
        CurrentRun.ref("world")
        return "world"
