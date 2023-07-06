"""
Prism project
"""

# Imports
import os
import logging
from pathlib import Path
from prism.admin import generate_run_id, generate_run_slug


# sys.path config. This gives your tasks access to local tasks / packages that exist
# outside of your project structure.
SYS_PATH_CONF = [
    Path(__file__).parent,
]

# Run ID and slug
RUN_ID = generate_run_id()
SLUG = generate_run_slug()

# Thread count: number of workers to use to execute tasks concurrently. If set to 1,
# then 1 task is run at a time.
THREADS = os.cpu_count()

# Logger
PRISM_LOGGER = logging.getLogger("PRISM_LOGGER")


# Other variables / parameters. Make sure to capitalize all of these!
VAR_1 = {'a': 'b'}
VAR_2 = 200
VAR_3 = '2015-01-01'

# Paths
WKDIR = Path(__file__).parent
DATA = WKDIR / 'data'
OUTPUT = WKDIR / 'output'


# Triggers
TRIGGERS = {
    'on_success': ["test_trigger_good_structure"],
    'on_failure': ["test_trigger_good_structure"],
}
