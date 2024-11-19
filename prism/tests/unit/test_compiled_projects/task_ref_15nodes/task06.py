import prism.task
from prism.runtime import Ref


class Task06(prism.task.PrismTask):
    def run(self):
        return Ref("task05.Task05") + "This is task 06. "
