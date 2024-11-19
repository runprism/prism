import prism.target
from prism.decorators import target, task
from prism.runtime import Context


@task(
    task_id="example-decorated-task",
    targets=[target(type=prism.target.Txt, loc=Context("OUTPUT") / "hello_world.txt")],
)
def example_task():
    return "Hello, world!"
