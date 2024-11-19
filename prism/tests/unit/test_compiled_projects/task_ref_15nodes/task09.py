import prism.task
from prism.runtime import Ref


class Task09(prism.task.PrismTask):
    def run(self):
        return Ref("task05.Task05") + Ref("task08.Task08") + "This is task 09. "  # noqa: E501
