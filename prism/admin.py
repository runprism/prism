"""
Functions for DAG run admin variables
"""

# Imports
import coolname
import uuid
import shortuuid

# Words to ignore and exclude from slug
IGNORE_LIST = {
    "sexy",
    "demonic",
    "kickass",
    "heretic",
    "godlike",
    "booby",
    "chubby",
    "gay",
    "sloppy",
    "funky",
    "juicy",
    "beaver",
    "curvy",
    "fat",
    "flashy",
    "flat",
    "thick",
    "nippy",
}


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

    # Regenerate words if they include ignored words
    while IGNORE_LIST.intersection(slug.split("-")):
        slug = coolname.generate_slug(2)

    return f'{slug}-{uuid_short.replace("-", "")}'
