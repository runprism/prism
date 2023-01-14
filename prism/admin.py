"""
Functions for DAG run admin variables
"""

# Imports
import coolname
import uuid
import shortuuid


# Functions
def generate_run_id() -> str:
    """
    Cryptographically secure run ID (using UUID)
    """
    return str(uuid.uuid4()).replace("-", "")


def generate_run_slug() -> str:
    """
    Run slug concatenated with a short UUID. Not necessarily cryptographically secure.
    We offer this in addition to the run ID because it's more human-readable.
    """
    uuid_short = str(shortuuid.ShortUUID().random(length=10))
    slug = coolname.generate_slug(2)
    return f'{slug}-{uuid_short.replace("-", "")}'
