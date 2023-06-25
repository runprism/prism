"""
BaseEventManager class definition

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Standard library imports
from dataclasses import dataclass
import sys
import time
from typing import Any, Callable, List, Optional, Union

# Prism-specific imports
import prism.prism_logging
import prism.exceptions
from prism.prism_logging import Event, fire_console_event, fire_empty_line_event
from prism.ui import RED, GREEN, EVENT_COLOR, RESET


####################
# Class definition #
####################

@dataclass
class EventManagerOutput:
    outputs: Any
    event_to_fire: Optional[prism.prism_logging.Event]
    event_list: List[prism.prism_logging.Event]


class BaseEventManager:
    """
    Class for managing logging events fired by tasks and associated
    functions
    """

    def __init__(self,
        idx: Union[None, int],
        total: Union[None, int],
        name: str,
        full_tb: bool,
        func: Callable[..., Any]
    ):
        self.idx = idx
        self.total = total
        self.name = name
        self.full_tb = full_tb
        self.func = func

    def fire_running_exec_event(self,
        event_list: List[prism.prism_logging.Event]
    ):
        """
        Create ExecutionEvent informing user of task execution
        """
        e = prism.prism_logging.ExecutionEvent(
            msg=f"RUNNING EVENT {EVENT_COLOR}'{self.name}'{RESET}",
            num=self.idx,
            total=self.total,
            status="RUN",
            execution_time=None
        )
        event_list = fire_console_event(e, event_list, log_level='info')
        return event_list

    def fire_success_exec_event(self,
        start_time: float,
        event_list: List[prism.prism_logging.Event]
    ) -> List[Event]:
        """
        Create ExecutionEvent informing user of successful task execution
        """
        execution_time = time.time() - start_time
        e = prism.prism_logging.ExecutionEvent(
            msg=f"{GREEN}FINISHED{RESET} EVENT {EVENT_COLOR}'{self.name}'{RESET}",
            num=self.idx,
            total=self.total,
            status="DONE",
            execution_time=execution_time
        )
        event_list = fire_console_event(e, event_list, log_level='info')
        return event_list

    def fire_error_exec_event(self,
        start_time: float,
        event_list: List[prism.prism_logging.Event]
    ) -> List[Event]:
        """
        Create ExecutionEvent informing user of error in task execution
        """
        execution_time = time.time() - start_time
        e = prism.prism_logging.ExecutionEvent(
            msg=f"{RED}ERROR{RESET} IN EVENT {EVENT_COLOR}'{self.name}'{RESET}",
            num=self.idx,
            total=self.total,
            status="ERROR",
            execution_time=execution_time
        )

        # Set the log-level to `info`. We'll fire the actual error using log-level
        # `error`.
        event_list = fire_console_event(e, event_list, log_level='error')
        return event_list

    def run(self, **kwargs):
        """
        Execute task using inputted function inputted during initialization
        """
        return self.func(**kwargs)

    def manage_events_during_run(self,
        event_list: List[prism.prism_logging.Event],
        fire_exec_events=True,
        fire_empty_line_events=True,
        **kwargs
    ) -> EventManagerOutput:
        """
        Fire relevant event managers
        """

        start_time = time.time()
        if fire_exec_events:
            event_list = self.fire_running_exec_event(event_list)

        # Execute task
        try:
            outputs = self.run(**kwargs)
            if fire_exec_events:
                event_list = self.fire_success_exec_event(start_time, event_list)

            # Return output of task execution
            return EventManagerOutput(outputs, None, event_list)

        # If PrismException, then create PrismExceptionErrorEvent
        except prism.exceptions.PrismException as err:
            if fire_exec_events:
                event_list = self.fire_error_exec_event(start_time, event_list)
            if fire_empty_line_events:
                event_list = fire_empty_line_event(event_list)
            prism_exception_event = prism.prism_logging.PrismExceptionErrorEvent(
                err, self.name
            )
            return EventManagerOutput(0, prism_exception_event, event_list)

        # If SyntaxError, then create ExecutionSyntaxErrorEvent
        except SyntaxError:
            if fire_exec_events:
                event_list = self.fire_error_exec_event(start_time, event_list)
            if fire_empty_line_events:
                event_list = fire_empty_line_event(event_list)
            exc_type, exc_value, exc_tb = sys.exc_info()
            if self.full_tb:
                syntax_error_event = prism.prism_logging.ExecutionSyntaxErrorEvent(
                    self.name, exc_type, exc_value, exc_tb, True
                )
            else:
                syntax_error_event = prism.prism_logging.ExecutionSyntaxErrorEvent(
                    self.name, exc_type, exc_value, exc_tb, False
                )
            return EventManagerOutput(0, syntax_error_event, event_list)

        # If any other Exception, then create ExecutionErrorEvent
        except Exception:
            if fire_exec_events:
                event_list = self.fire_error_exec_event(start_time, event_list)
            if fire_empty_line_events:
                event_list = fire_empty_line_event(event_list)
            exc_type, exc_value, exc_tb = sys.exc_info()
            if self.full_tb:
                exception_event = prism.prism_logging.ExecutionErrorEvent(
                    self.name, exc_type, exc_value, exc_tb, True
                )
            else:
                exception_event = prism.prism_logging.ExecutionErrorEvent(
                    self.name, exc_type, exc_value, exc_tb, False
                )
            return EventManagerOutput(0, exception_event, event_list)
