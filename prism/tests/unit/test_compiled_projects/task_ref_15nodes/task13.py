import prism.task
from prism.runtime import Ref


class Task13(prism.task.PrismTask):
    def run(self):
        return Ref("task10.Task10") + "This is task 13. "
