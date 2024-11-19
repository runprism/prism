import prism.task
from prism.runtime import Ref


class Task03(prism.task.PrismTask):
    def run(self):
        return Ref("task01.Task01") + "This is task 03. "
