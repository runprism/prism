import prism.decorators
import prism.task
from prism.runtime import CurrentRun


# Class-based task
class Task07a(prism.task.PrismTask):
    def run(self):
        return (
            CurrentRun.ref("task04.Task04")
            + CurrentRun.ref("task06.Task06")
            + "This is task 07. "
        )  # noqa: E501


# Function-based task
@prism.decorators.task()
def task_07b():
    _ = CurrentRun.ref("task07.Task07a")
    return "This is a local task"
