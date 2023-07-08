# Module list
from pathlib import Path
DAG_CYCLE_MODULE_LIST = [
    Path(f"module{l}.py") for l in ['A', 'B', 'C', 'D', 'E']  # noqa
]

DAG_CYCLE_TASK_LIST = [
    f"module{l}.Task{l.lower()}" for l in ['A', 'B', 'C', 'D', 'E']  # noqa
]
