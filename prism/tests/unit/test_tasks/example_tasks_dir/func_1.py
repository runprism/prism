from prism.task import PrismTask
from prism.runtime import CurrentRun


class Func1(PrismTask):
    task_id = "func1"

    def run(self):
        CurrentRun.ref("func0")
        return "world"
