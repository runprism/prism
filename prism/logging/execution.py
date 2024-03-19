import time
from typing import Any, Callable, Optional

# Prism-specific imports
from prism.logging.events import ExecutionEvent
import prism.logging.events
from prism.task import PrismTask


class _ExecutionEventManager:
    """
    For certain actions, we fire events to indicate before the action starts to indicate
    that the action is taking place and after the action ends to indicate that the
    action succeeded/failed. This class helps us manage that.
    """

    def __init__(
        self,
        idx: Optional[int],
        total: Optional[int],
        name: str,
        func: Callable[..., Any],
    ):
        self.idx = idx
        self.total = total
        self.name = name
        self.func = func

    def fire_skipped_exec_event(self, is_task: bool = False) -> None:
        """
        Create ExecutionEvent informing user that a task was skipped
        """
        event_with_formatting = (
            f"TASK [blue]{self.name}[/blue]" if is_task else self.name.lower()
        )  # noqa: E501
        e = ExecutionEvent(
            msg=f"[orange1]SKIPPING[/orange1] {event_with_formatting}",
            num=self.idx,
            total=self.total,
            status="SKIP",
            execution_time=None,
        )
        prism.logging.events.fire_console_event(e, log_level="info")
        return None

    def fire_running_exec_event(self, is_task: bool = False) -> None:
        """
        Create ExecutionEvent informing user of task execution
        """
        event_with_formatting = (
            f"RUNNING TASK [blue]{self.name}[/blue]" if is_task else self.name
        )  # noqa: E501
        e = ExecutionEvent(
            msg=event_with_formatting,
            num=self.idx,
            total=self.total,
            status="RUN",
            execution_time=None,
        )
        prism.logging.events.fire_console_event(e, log_level="info")
        return None

    def fire_success_exec_event(self, start_time: float, is_task: bool = False) -> None:
        """
        Create ExecutionEvent informing user of successful task execution
        """
        event_with_formatting = (
            f"TASK [blue]{self.name}[/blue]" if is_task else self.name.lower()
        )  # noqa: E501
        execution_time = time.time() - start_time
        e = ExecutionEvent(
            msg=f"[green]FINISHED[/green] {event_with_formatting}",
            num=self.idx,
            total=self.total,
            status="DONE",
            execution_time=execution_time,
        )
        prism.logging.events.fire_console_event(e, log_level="info")
        return None

    def fire_error_exec_event(self, start_time: float, is_task: bool = False) -> None:
        """
        Create ExecutionEvent informing user of error in task execution
        """
        event_with_formatting = (
            f"IN TASK [blue]{self.name}[/blue]" if is_task else self.name.lower()
        )  # noqa: E501
        execution_time = time.time() - start_time
        e = ExecutionEvent(
            msg=f"[red]ERROR[/red] {event_with_formatting}",
            num=self.idx,
            total=self.total,
            status="ERROR",
            execution_time=execution_time,
        )
        prism.logging.events.fire_console_event(e, log_level="error")
        return None

    def run(
        self, fire_exec_events: bool = True, is_task: bool = False, **kwargs
    ) -> Any:
        start_time = time.time()
        if fire_exec_events:
            self.fire_running_exec_event(is_task)

        # The only events we ever really skip are actual tasks. For these, the skip
        # logic is handled within the task's `exec` function. So, we just run it
        # normally here.
        try:
            output = self.func(**kwargs)

            # Check if the output is a task manager. If it is, then we've run a task.
            # Check if the task was skipped, and fire the corresponding event.
            if isinstance(output, PrismTask):
                if fire_exec_events:
                    if output.is_done:
                        self.fire_skipped_exec_event(is_task)
                    else:
                        self.fire_success_exec_event(start_time, is_task)
            elif fire_exec_events:
                self.fire_success_exec_event(start_time, is_task)
            return output
        except Exception:
            self.fire_error_exec_event(start_time, is_task)
            raise
