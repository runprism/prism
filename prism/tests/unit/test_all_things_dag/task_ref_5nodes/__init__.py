# Module list
from pathlib import Path
TASK_REF_5NODES_MODULE_LIST = [
    Path(f"task{l}.py") for l in ['A', 'B', 'C', 'D', 'E']  # noqa
]

TASK_REF_5NODES_TASK_LIST = [
    f"task{l}.Task{l.lower()}" for l in ['A', 'B', 'C', 'D', 'E']  # noqa
]
