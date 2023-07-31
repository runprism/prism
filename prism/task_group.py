"""
PrismTaskGroup class definition. Task groups are automatically inferred from the
directory structure (i.e., all tasks in a subfolder are assumed to be in the same
group). The PrismTaskGroup class allows users to customize the behavior of task groups
(e.g., number of retries, seconds between retries, actions to take before each retry).
"""

# Imports
from typing import List

# Prism imports
from prism.infra.task_manager import PrismTaskManager
from prism.infra.hooks import PrismHooks


# Class definition
class PrismTaskGroup:

    RETRIES: int
    RETRY_DELAY_SECONDS: int
    TASK_LIST: List[str]

    def before_retry(self,
        task: PrismTaskManager,
        hooks: PrismHooks,
    ):
        """
        Custom actions to perform before the task group is retried
        """
        return
