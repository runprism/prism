import prism.task
from prism.runtime import Ref


class Task02(prism.task.PrismTask):
    def run(self):
        return Ref("task01.Task01") + "This is task 02."
