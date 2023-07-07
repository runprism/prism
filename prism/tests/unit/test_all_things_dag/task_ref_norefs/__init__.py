# Module list
from pathlib import Path
TASK_REF_NOREFS_MODULE_LIST = [
    Path(f"module{l}.py") for l in ['A', 'B', 'C', 'D', 'E']  # noqa
]

TASK_REF_NOREFS_TASK_LIST = [
    f"module{l}.Task{l.lower()}" for l in ['A', 'B', 'C', 'D', 'E']  # noqa
]
