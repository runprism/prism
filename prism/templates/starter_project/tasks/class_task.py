import prism.task
import prism.target
import prism.decorators
from prism.runtime import CurrentRun


class ExampleTask(prism.task.PrismTask):
    task_id = "example-class-task"

    # Run
    @prism.decorators.target(
        type=prism.target.Txt, loc=CurrentRun.ctx("OUTPUT") / "hello_world.txt"
    )
    def run(self):
        return "Hello, world!"
