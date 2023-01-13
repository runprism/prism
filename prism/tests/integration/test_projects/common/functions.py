"""
Common functions for Prism project integration tests.
"""

# Imports
from pathlib import Path


# Functions
def test_trigger_function(project_name: str):
    output_dir = Path(__file__).parent.parent / project_name / 'output'
    with open(output_dir / 'trigger.txt', 'w') as f:
        f.write('This is outputted from the trigger function!')
