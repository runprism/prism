# Module list
from pathlib import Path
TASK_REF_3NODES_MODULE_LIST = [
    Path(f"task0{n}.py") for n in range(1, 4)
]

TASK_REF_3NODES_TASK_LIST = [
    f"task0{n}.Task0{n}" for n in range(1, 4)
]
