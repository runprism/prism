import prism.task
from prism.runtime import Ref


class Task11(prism.task.PrismTask):
    def run(self):
        return Ref("task07.Task07a") + Ref("task10.Task10") + "This is task 11."  # noqa: E501
