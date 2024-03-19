from prism.task import PrismTask
from prism.runtime import CurrentRun


class World(PrismTask):
    task_id = "world"

    def run(self):
        CurrentRun.ref("hello")
        return "world"
