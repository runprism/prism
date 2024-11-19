from pathlib import Path

import prism.target
from prism.decorators import target, task
from prism.runtime import Ref


@task(targets=[target(type=prism.target.Txt, loc=Path(__file__) / "test.txt")])
def task_with_refs():
    _ = Ref("hello")
    _ = Ref("world")
    _ = Ref("func_0")
    _ = Ref("func_1")
    return "hi"
