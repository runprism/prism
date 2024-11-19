import prism.task
from prism.runtime import Ref


class Task03(prism.task.PrismTask):
    def run(self):
        return Ref("task02.Task02") + "This is task 3."
