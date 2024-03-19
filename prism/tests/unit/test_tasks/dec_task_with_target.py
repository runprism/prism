from prism.decorators import task, target
import prism.target
from pathlib import Path
from prism.runtime import CurrentRun


@task(targets=[target(type=prism.target.Txt, loc=Path(__file__) / "test.txt")])
def task_with_target():
    _ = CurrentRun.ref("hello.py")
    _ = CurrentRun.ref("world.py")
    return "hi"
