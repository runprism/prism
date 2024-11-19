from prism.runtime import Ref
from prism.task import PrismTask


class World(PrismTask):
    task_id = "world"

    def run(self):
        Ref("hello")
        return "world"
