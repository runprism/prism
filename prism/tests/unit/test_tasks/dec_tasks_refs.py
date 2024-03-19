from prism.decorators import task, target
import prism.target
from pathlib import Path
from prism.runtime import CurrentRun


@task(targets=[target(type=prism.target.Txt, loc=Path(__file__) / "test.txt")])
def task_with_refs():
    _ = CurrentRun.ref("hello")
    _ = CurrentRun.ref("world")
    _ = CurrentRun.ref("func_0")
    _ = CurrentRun.ref("func_1")
    return "hi"
