# Module list
from pathlib import Path
TASK_REF_15NODES_MODULE_LIST = [
    Path(f"task0{n}.py") if len(str(n)) == 1 else Path(f"task{n}.py") for n in range(1, 16)  # noqa
]

TASK_REF_15NODES_TASK_LIST = [
    f"task0{n}.Task0{n}" if len(str(n)) == 1 else f"task{n}.Task{n}" for n in range(1, 16)  # noqa
]
idx = TASK_REF_15NODES_TASK_LIST.index("task07.Task07")
TASK_REF_15NODES_TASK_LIST.pop(idx)
TASK_REF_15NODES_TASK_LIST += ["task07.Task07a", "task07.task_07b"]
