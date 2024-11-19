from pathlib import Path

import prism.target
from prism.decorators import target, task
from prism.runtime import Ref


@task(targets=[target(type=prism.target.Txt, loc=Path(__file__) / "test.txt")])
def task_with_target():
    _ = Ref("hello.py")
    _ = Ref("world.py")
    return "hi"
