from prism.decorators import task, target
import prism.target
from pathlib import Path


def helper_function_1(tasks):
    return tasks.ref("helper_function1.py")


@task(
    targets=[
        target(type=prism.target.Txt, loc=Path(__file__) / 'test.txt')
    ]
)
def task_with_refs(tasks, hooks):
    _ = tasks.ref('hello.py')
    _ = tasks.ref('world.py')
    return "hi"


def helper_function_2(tasks):
    return tasks.ref("helper_function2.py")
