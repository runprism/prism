import prism.decorators
import prism.target
import prism.task
from prism.runtime import Context


class ExampleTask(prism.task.PrismTask):
    task_id = "example-class-task"

    # Run
    @prism.decorators.target(
        type=prism.target.Txt, loc=Context("OUTPUT") / "hello_world.txt"
    )
    def run(self):
        return "Hello, world!"
