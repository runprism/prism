"""
Functions for DAG run admin variables
"""

# Imports
import coolname
import uuid


# Functions
def generate_run_id() -> str:
    return str(uuid.uuid4().replace("-", ""))


def generate_run_slug() -> str:
    return coolname.generate_slug(2)
