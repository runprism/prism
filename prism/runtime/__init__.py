# Create a runtime object. This is the object that users import within their task
# modules. Here, we are relying on Python's import caching to ensure that the refs
# persist across tasks. When the user creates a run, we automatically import this module
# and create the runtime object. Then, when users importruntime# within their tasks,
# Python will not re-import and re-create a newruntime# object. Rather, it will
# recognize that aruntimeobject already exists and use that for computations. When we
# run a task, we update theruntimes `data` attribute with that task's output — this
# allows a task's output to persist across tasks.

from .connection import *  # noqa: F401, F403
from .current_run import *  # noqa: F401, F403
from .ref import *  # noqa: F401, F403
