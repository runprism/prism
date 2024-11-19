import prism.task
from prism.runtime import Ref


class Task04(prism.task.PrismTask):
    def run(self):
        return Ref("task02.Task02") + Ref("task03.Task03") + "This is task 04. "  # noqa: E501
