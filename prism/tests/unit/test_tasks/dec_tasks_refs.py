from prism.decorators import task, target
import prism.target
from pathlib import Path


@task(
    targets=[
        target(type=prism.target.Txt, loc=Path(__file__) / 'test.txt')
    ]
)
def task_with_refs(tasks, hooks):
    _ = tasks.ref('hello.py')
    _ = tasks.ref('world.py')
    _ = tasks.ref("func_0.py")
    _ = tasks.ref("func_1.py")
    return "hi"
