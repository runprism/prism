"""
BaseEventManager class definition

Table of Contents
- Imports
- Class definition
"""

#############
## Imports ##
#############

# Standard library imports
import argparse
import sys
import time
from typing import Callable, List

# Prism-specific imports
import prism.logging
import prism.exceptions
from prism.logging import fire_console_event, fire_empty_line_event
from prism.ui import BLACK, RED, GREEN, YELLOW, BLUE, PURPLE, CYAN, WHITE, RESET, BRIGHT_WHITE, BRIGHT_YELLOW, BRIGHT_GREEN, BOLD
from typing import Any, Union


######################
## Class definition ##
######################

class BaseEventManager:
    """
    Class for managing logging events fired by tasks and associated
    functions
    """

    def __init__(self,
        args: argparse.Namespace,
        idx: Union[None, int],
        total: Union[None, int],
        name: str,
        func: Callable[..., Any]
    ):
        self.args = args
        self.idx = idx
        self.total = total
        self.name = name
        self.func = func


    def fire_running_exec_event(self,
        event_list: List[prism.logging.Event]
    ):
        """
        Create ExecutionEvent informing user of task execution
        """
        e = prism.logging.ExecutionEvent(
            msg=f"RUNNING {BLUE}{self.name}{RESET}",
            num=self.idx,
            total=self.total,
            status="RUN",
            execution_time=None
        )
        event_list = fire_console_event(e, event_list)
        return event_list


    def fire_success_exec_event(self,
        start_time: float,
        event_list: List[prism.logging.Event]
    ):
        """
        Create ExecutionEvent informing user of successful task execution
        """
        execution_time = time.time() - start_time
        e = prism.logging.ExecutionEvent(
            msg=f"{GREEN}FINISHED{RESET} {BLUE}{self.name}{RESET}",
            num=self.idx,
            total=self.total,
            status="DONE",
            execution_time=execution_time
        )
        event_list = fire_console_event(e, event_list)
        return event_list


    def fire_error_exec_event(self,
        start_time: float,
        event_list: List[prism.logging.Event]
    ):
        """
        Create ExecutionEvent informing user of error in task execution
        """
        execution_time = time.time() - start_time
        e = prism.logging.ExecutionEvent(
            msg=f"{RED}ERROR{RESET} {BLUE}{self.name}{RESET}",
            num=self.idx,
            total=self.total,
            status="ERROR",
            execution_time=execution_time
        )
        event_list = fire_console_event(e, event_list)
        return event_list


    def run(self, **kwargs):
        """
        Execute task using inputted function inputted during initialization
        """
        return self.func(**kwargs)


    def manage_events_during_run(self, event_list: List[prism.logging.Event], fire_exec_events=True, **kwargs):
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
            return outputs, event_list
        
        # If PrismException, then create PrismExceptionErrorEvent
        except prism.exceptions.PrismException as err:
            if fire_exec_events:
                event_list = self.fire_error_exec_event(start_time, event_list)
            event_list = fire_empty_line_event(event_list)
            prism_exception_event = prism.logging.PrismExceptionErrorEvent(err, self.name)
            event_list = fire_console_event(prism_exception_event, event_list, 0)
            event_list = fire_console_event(prism.logging.SeparatorEvent(), event_list, 0)
            
            # Return 0 to indicate error
            return 0, event_list

        # If SyntaxError, then create ExecutionSyntaxErrorEvent
        except SyntaxError:
            if fire_exec_events:
                event_list = self.fire_error_exec_event(start_time, event_list)
            event_list = fire_empty_line_event(event_list)
            exc_type, exc_value, exc_tb = sys.exc_info()
            full_tb = self.args.full_tb
            if full_tb:
                syntax_error_event = prism.logging.ExecutionSyntaxErrorEvent(self.name, exc_type, exc_value, exc_tb, True)
            else:
                syntax_error_event = prism.logging.ExecutionSyntaxErrorEvent(self.name, exc_type, exc_value, exc_tb, False)
            event_list = fire_console_event(syntax_error_event, event_list, 0)
            event_list = fire_console_event(prism.logging.SeparatorEvent(), event_list, 0)
            
            # Return 0 to indicate error
            return 0, event_list
        
        # If any other Exception, then create ExecutionErrorEvent
        except Exception:
            if fire_exec_events:
                event_list = self.fire_error_exec_event(start_time, event_list)
            event_list = fire_empty_line_event(event_list)
            exc_type, exc_value, exc_tb = sys.exc_info()
            full_tb = self.args.full_tb
            if full_tb:
                exception_event = prism.logging.ExecutionErrorEvent(self.name, exc_type, exc_value, exc_tb, True)
            else:
                exception_event = prism.logging.ExecutionErrorEvent(self.name, exc_type, exc_value, exc_tb, False)
            event_list = fire_console_event(exception_event, event_list, 0)
            event_list = fire_console_event(prism.logging.SeparatorEvent(), event_list, 0)
            
            # Return 0 to indicate error
            return 0, event_list


# EOF