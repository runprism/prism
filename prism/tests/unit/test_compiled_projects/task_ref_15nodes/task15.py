import prism.task
from prism.runtime import Ref


class Task15(prism.task.PrismTask):
    def run(self):
        return Ref("task11.Task11") + "This is task 15. "
