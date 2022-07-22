"""
Create logger for console events

Table of Contents
- Imports
- Create logger
- Functions and utils
- Event classes
"""

#############
## Imports ##
#############

# General package imports
import re
import os
import copy
import math
import logging
import time
from typing import List, Type, Union
from dataclasses import dataclass
from datetime import datetime
import traceback
import types
from typing import Any, Optional

# Prism imports
import prism.constants
import prism.exceptions
from prism.ui import BLACK, RED, GREEN, YELLOW, BLUE, PURPLE, CYAN, WHITE, RESET, BRIGHT_WHITE, BRIGHT_YELLOW, BRIGHT_GREEN, BOLD


###################
## Create logger ##
###################

DEFAULT_LOGGER = logging.getLogger('event_logger')
DEFAULT_LOGGER.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
console_formatter = logging.Formatter(fmt="%(message)s")
handler.setFormatter(console_formatter)
DEFAULT_LOGGER.addHandler(handler)


#########################
## Functions and utils ##
#########################

def colorize_status(status):
    """
    Colorize status; RUN should be gray, DONE should be green, ERROR should be red

    args:
        status: status (either RUN, DONE, ERROR)
    returns
        colorized_status: status with color
    """
    assert status in ["RUN", "DONE", "ERROR"], print(f"{status} is invalid; must be either RUN, DONE, or ERROR")
    if status=="RUN":
        return f"{YELLOW}RUN{RESET}"
    elif status=="DONE":
        return f"{GREEN}DONE{RESET}"
    elif status=="ERROR":
        return f"{RED}ERROR{RESET}"


def escape_ansi(string: str) -> str:
    """
    Replace ANSI escape codes with ''

    args:
        string: string containing ANSI codes
    returns:
        inputted string without ANSI codes
    """
    ansi_regex = re.compile('\x1b[^m]*m')
    return ansi_regex.sub('', string)


def custom_ljust(string: str, width: int, char: str) -> str:
    """
    Python's native `ljust` does not account for ANSI escape codes; create a custom ljust function for the console
    output.

    args:
        string: string to ljust
        width: width for ljust
        char: character to use for ljust
    returns:
        ljust applied input string after ignoring ANSI escape codes
    """
    # Regex pattern for ANSI codes
    ansi_regex = re.compile('\x1b[^m]*m')
    
    # ANSI matches
    matches = [(m.start(), m.end()) for m in re.finditer(ansi_regex, string)]

    # Replace ANSI matches with ''
    string_with_ansi_replaced = ansi_regex.sub('', string)

    # ljust
    string_ljust = string_with_ansi_replaced.ljust(width, char)

    # Add ANSI characters back in
    string_ljust_with_ansi = copy.deepcopy(string_ljust)
    for match in matches:
        start = match[0]
        end = match[1]
        string_ljust_with_ansi = string_ljust_with_ansi[:start] + string[start:end] + string_ljust_with_ansi[start:]
    return string_ljust_with_ansi


def format_console_output(message, index, total, status, execution_time):
    """
    Format message for console output
    """

    # Time
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")

    # Script number
    if index is None or total is None:
        progress = f'{PURPLE}{current_time}{RESET} | '
    else:
        progress = f'{PURPLE}{current_time}{RESET} | {index} of {total} '
    prefix = f"{progress}{message}"

    try:
        width = min(os.get_terminal_size(0)[0], prism.constants.TERMINAL_WIDTH)
    except:
        width = prism.constants.TERMINAL_WIDTH
    truncate_width = math.ceil(0.9*(width))
    justified = custom_ljust(prefix, width, ".")
    
    if len(escape_ansi(justified)) > width:
        justified = justified[:truncate_width]

    if execution_time is None:
        status_time = ""
    else:
        status_time = f" in {execution_time:0.2f}s"
    colorized_status = colorize_status(status)
    output = f"{justified} [{colorized_status}{status_time}]"
    return output


###################
## Event classes ##
###################

class Event:
    """
    Generic class for console events
    """

    def __str__(self) -> str:
        """
        String representation for an event. For now, we will use the event class name.
        """
        return self.__class__.__name__


    def message(self):
        raise prism.exceptions.ConsoleEventException(message='"message" function not implemented')


@dataclass
class CreatingProjectDirEvent(Event):

	def message(self):
	    return f'Creating project directory...'


@dataclass
class CurrentProjectDirEvent(Event):
    path: str
	
    def message(self):
	    return f'{BOLD}Found project directory at {YELLOW}{self.path}{RESET}'


@dataclass
class SettingUpProfileEvent(Event):

	def message(self):
	    return f'Setting up profile...'


@dataclass
class ProjectAlreadyExistsEvent(Event):
    path: str

    def message(self):
        return f'{RED}{self.path} already exists{RESET}'


@dataclass
class InitErrorEvent(Event):
    err: Union[SyntaxError, AttributeError, Exception]

    def message(self):
        return f'{RED}{self.err.args[0]}{RESET}'


@dataclass
class InvalidProfileType(Event):
    type: Union[None, str]

    def message(self):
        if self.type is None:
            return f'{RED}Specify profile type with --type arg{RESET}'
        else:
            return f'{RED}Invalid profile type; must be one of "snowflake", "pyspark", or "dbt"{RESET}'


@dataclass
class ProfileAlreadyExists(Event):
    path: str

    def message(self):
        return f'{RED}{self.path} already exists{RESET}'
    

@dataclass
class ProfileNameExistsYamlDoesNotExist(Event):

    def message(self):
        return f'{YELLOW}WARNING: profile name found in prism_project.py but profile.yml not found{RESET}'


@dataclass
class ProfileNameDoesNotExistYamlExists(Event):

    def message(self):
        return f'{YELLOW}WARNING: profile.yml found but profile name not found prism_project.py{RESET}'


@dataclass
class InitSuccessfulEvent(Event):
	msg: str

	def message(self):
	    return self.msg


@dataclass
class DependencyErrorEvent(Event):

    def message(self):
        pass


@dataclass
class TaskRunEvent(Event):
	version: str

	def message(self):
		return f'{BOLD}Running with prism {YELLOW}v{self.version}...{RESET}'


@dataclass
class TaskSuccessfulEndEvent(Event):
	
	def message(self):
	    return f'{BRIGHT_GREEN}Done!{RESET}'


@dataclass
class ProjectPyNotFoundEvent(Event):
    err: prism.exceptions.ProjectPyNotFoundException
    
    def message(self):
        error_class = self.err.__class__.__name__
        detail = self.err.args[0]
        msg = "%s: %s" % (error_class, detail)
        return f'{RED}{msg}{RESET}'


@dataclass
class EmptyLineEvent(Event):
	
	def message(self):
	    return ' '


@dataclass
class ModulesFolderNotFoundEvent(Event):
    path: str
	
    def message(self):
        return f'{RED}`modules` subfolder not found in {self.path}{RESET}'


@dataclass
class SeparatorEvent(Event):

    def message(self):
        try:
            width = min(os.get_terminal_size(0)[0], prism.constants.TERMINAL_WIDTH)
        except:
            width = prism.constants.TERMINAL_WIDTH
        truncate_width = math.ceil(0.9*(width))
        justified = "---".ljust(width, "-")
        if len(justified) > width:
            justified = justified[:truncate_width]
        return justified


@dataclass
class CompileStartEvent(Event):
    modules: int
    task_desc: str

    def message(self):
        return f'{BOLD}Found {self.modules} modules to {self.task_desc}{RESET}'


@dataclass
class CompileExecutionEvent(Event):
    msg: str
    num: int
    total: int
    status: str
    execution_time: int

    def message(self):
        return format_console_output(
            self.msg,
            self.num,
            self.total,
            self.status,
            self.execution_time
        )


@dataclass
class ExecutionEvent(Event):
    msg: str
    num: Optional[int]
    total: Optional[int]
    status: str
    execution_time: Optional[float]
    
    
    def __str__(self):
        """
        ExecutionEvent messages are either:
            RUNNING {event.name}
            FINISHED {event.name}
            ERROR {event.name}   
           
        Add name of event (after removing event status and ANSI codes) to string representation of event.
        """
        # Remove all ANSI codes
        msg_no_ansi = escape_ansi(self.msg)

        # Remove the event status
        status_regex = re.compile('(RUNNING|FINISHED|ERROR)')
        msg_no_ansi_status = status_regex.sub('', msg_no_ansi)

        # Remove all trailing / leading spaces
        event_name_str = msg_no_ansi_status.strip()
        return super().__str__() + " - " + event_name_str + " - " + self.status


    def message(self):
        return format_console_output(
            self.msg,
            self.num,
            self.total,
            self.status,
            self.execution_time
        )


@dataclass
class ExecutionErrorEvent(Event):
    name: str
    type: Optional[Any]
    value: Optional[BaseException]
    tb: Optional[types.TracebackType]
    full_tb: bool

    def prepare_initial_tb(self) -> List[str]:
        if self.full_tb:
            tb_stack = traceback.format_exception(self.type, self.value, self.tb, limit=None)
            return tb_stack
        else:
            tb_stack = traceback.format_exception(self.type, self.value, self.tb, limit=-1)
            idx = tb_stack.index('Traceback (most recent call last):\n')
            tb_stack.insert(idx+1, '...\n...\n')
            return tb_stack
        

    def message(self):
        tb_stack = self.prepare_initial_tb()
        msg = f"{RED}{''.join(tb_stack)}{RESET}"
        msg = re.sub('"<[a-z\s\/]+>"', self.name, msg)
        return msg


@dataclass
class ExecutionSyntaxErrorEvent(ExecutionErrorEvent):

    def message(self):
        tb_stack = self.prepare_initial_tb()
        exec_regex = "exec\(.*\)"
        tb_stack = [statement for statement in tb_stack if len(re.findall(exec_regex, statement))==0]
        msg = f"{RED}{''.join(tb_stack)}{RESET}"
        msg = re.sub('"<[a-z\s\/]+>"', self.name, msg)
        return msg


@dataclass
class PrismExceptionErrorEvent(Event):
    err: prism.exceptions.PrismException
    name: str
    
    def message(self):
        error_class = self.err.__class__.__name__
        detail = self.err.args[0]
        msg = "%s in `%s`: %s" % (error_class, self.name, detail)
        return f'{RED}{msg}{RESET}'


def fire_console_event(Event, event_list: List[Event] = [], sleep=0.25):
    msg = Event.message()
    DEFAULT_LOGGER.info(msg)
    time.sleep(sleep)

    # Return event list
    event_list.append(Event)
    return event_list


def fire_empty_line_event(event_list: List[Event] = []):
    e = EmptyLineEvent()
    msg = e.message()
    DEFAULT_LOGGER.info(msg)

    # Return event list
    event_list.append(e)
    return event_list


# EOF