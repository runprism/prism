"""Entrypoint for your Prism project.
"""

from prism.client import PrismProject
from pathlib import Path


# Project
project = PrismProject(
    version="1.0",
    tasks_dir=Path.cwd() / "tasks",
    concurrency=2,
    ctx={
        "OUTPUT":  Path.cwd() / "output"
    },
)


# Run
if __name__ == "__main__":
    project.run()
