"""
Create logger for console events

Table of Contents
- Imports
- Functions and utils
- Create logger
- Event classes
"""

###########
# Imports #
###########

# General package imports
import argparse
import re
import os
import copy
import math
import logging
import time
from typing import List, Union
from dataclasses import dataclass
from datetime import datetime
import traceback
import types
from typing import Any, Optional
import functools
import warnings

# Prism imports
import prism.constants
import prism.exceptions
from prism.ui import (
    RED,
    GREEN,
    YELLOW,
    RESET,
    BRIGHT_GREEN,
    BOLD,
    CYAN,
    MAGENTA,
    GRAY,
    GRAY_PINK,
    TERMINAL_WIDTH,
)


#######################
# Functions and utils #
#######################

def colorize_status(status):
    """
    Colorize status; RUN should be gray, DONE should be green, ERROR should be red

    args:
        status: status (either RUN, DONE, ERROR)
    returns
        colorized_status: status with color
    """
    if status not in ["RUN", "DONE", "ERROR"]:
        raise ValueError(f"{status} is invalid; must be either RUN, DONE, or ERROR")
    if status == "RUN":
        return f"{YELLOW}RUN{RESET}"
    elif status == "DONE":
        return f"{GREEN}DONE{RESET}"
    elif status == "ERROR":
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
    Python's native `ljust` does not account for ANSI escape codes; create a custom
    ljust function for the console output.

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
        string_ljust_with_ansi = string_ljust_with_ansi[:start] + \
            string[start:end] + \
            string_ljust_with_ansi[start:]
    return string_ljust_with_ansi


def format_console_output(message, log_level="info", formatted: bool = True):
    """
    Format message for console output
    """

    # Time
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")

    # Formatted log level
    if not formatted:
        return message
    elif log_level == "info":
        log_level_formatted = f'{CYAN}INFO {RESET}'
    elif log_level == "warn":
        log_level_formatted = f'{YELLOW}WARN {RESET}'
    elif log_level:
        log_level_formatted = f'{RED}{log_level.upper()}{RESET}'

    progress = f'{current_time}{RESET} | {log_level_formatted} | '
    formatted_msg = f"{progress}{message}"
    return formatted_msg


#################
# Create logger #
#################

DEFAULT_LOGGER: logging.Logger


def set_up_logger(args: argparse.Namespace):
    if globals().get('DEFAULT_LOGGER', None) is None:
        global DEFAULT_LOGGER

        DEFAULT_LOGGER = logging.getLogger('PRISM_LOGGER')

        def _set_level(obj, level: str):
            if level == 'info':
                obj.setLevel(logging.INFO)
            elif level == 'warn':
                obj.setLevel(logging.WARN)
            elif level == 'error':
                obj.setLevel(logging.ERROR)
            elif level == 'critical':
                obj.setLevel(logging.CRITICAL)
            return obj

        # Set the appropriate log level
        DEFAULT_LOGGER = _set_level(DEFAULT_LOGGER, args.log_level)

        # Stream handler
        handler = logging.StreamHandler()
        handler = _set_level(handler, args.log_level)
        console_formatter = logging.Formatter(fmt="%(message)s")
        handler.setFormatter(console_formatter)

        # File handler -- remove ANSI codes
        class FileHandlerFormatter(logging.Formatter):
            def format(self, record):
                return escape_ansi(record.msg)

        file_handler = logging.FileHandler('logs.log')
        file_handler = _set_level(file_handler, args.log_level)
        file_handler.setLevel(logging.INFO)
        file_handler_formatter = FileHandlerFormatter(fmt="%(message)s")
        file_handler.setFormatter(file_handler_formatter)

        # Add handlers
        DEFAULT_LOGGER.addHandler(file_handler)
        DEFAULT_LOGGER.addHandler(handler)


#################
# Event classes #
#################

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
        raise prism.exceptions.ConsoleEventException(
            message='"message" function not implemented'
        )


@dataclass
class CreatingProjectDirEvent(Event):

    def message(self):
        return 'Creating project directory...'


@dataclass
class CurrentProjectDirEvent(Event):
    path: str

    def message(self):
        return f'{BOLD}Found project directory at {MAGENTA}{self.path}{RESET}'


@dataclass
class SettingUpProfileEvent(Event):

    def message(self):
        return 'Setting up profile...'


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
class InvalidType(Event):
    arg_type: str
    valid_types: List[str]
    type: Optional[str] = None

    def message(self):
        if self.type is None:
            return f'{RED}Specify {self.arg_type} type with --type arg{RESET}'
        else:
            valid_types_str = ','.join(f'`{a}`' for a in self.valid_types)
            return f'{RED}Invalid {self.arg_type} type; must be one of {valid_types_str}{RESET}'  # noqa: E501


@dataclass
class ProfileAlreadyExists(Event):
    path: str

    def message(self):
        return f'{RED}{self.path} already exists{RESET}'


@dataclass
class ProfileNameExistsYamlDoesNotExist(Event):

    def message(self):
        return f'{YELLOW}`PROFILE` var found in prism_project.py but profile.yml not found{RESET}'  # noqa: E501


@dataclass
class ProfileNameExistsNamedProfileDoesNotExist(Event):

    def message(self):
        return f'{YELLOW}`PROFILE` var found in prism_project.py but named profile not found in profile.yml{RESET}'  # noqa: E501


@dataclass
class ProfileNameDoesNotExistYamlExists(Event):

    def message(self):
        return f'{YELLOW}profile.yml found but `PROFILE` var not found in prism_project.py{RESET}'  # noqa: E501


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
        return f'{BOLD}Running with prism {MAGENTA}v{self.version}...{RESET}'


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
            width = min(os.get_terminal_size(0)[0], TERMINAL_WIDTH)
        except Exception:
            width = TERMINAL_WIDTH
        truncate_width = math.ceil(0.9 * (width))
        justified = "---".ljust(width, "-")
        if len(justified) > width:
            justified = justified[:truncate_width]
        return justified


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

        Add name of event (after removing event status and ANSI codes) to string
        representation of event.
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
        message = self.msg
        if self.num is not None and self.total is not None:
            message = f'{self.num} of {self.total} {self.msg}'

        # Add execution time and status
        try:
            width = min(os.get_terminal_size(0)[0], TERMINAL_WIDTH)
        except Exception:
            width = TERMINAL_WIDTH
        truncate_width = math.ceil(0.9 * (width))
        justified = custom_ljust(message, width, ".")

        if len(escape_ansi(justified)) > width:
            justified = justified[:truncate_width]

        if self.execution_time is None:
            status_time = ""
        else:
            status_time = f" in {self.execution_time:0.2f}s"
        colorized_status = colorize_status(self.status)
        output = f"{justified} [{colorized_status}{status_time}]"
        return output


@dataclass
class ExecutionErrorEvent(Event):
    name: str
    type: Optional[Any]
    value: Optional[BaseException]
    tb: Optional[types.TracebackType]
    full_tb: bool

    def prepare_initial_tb(self) -> List[str]:
        if self.full_tb:
            tb_stack = traceback.format_exception(
                self.type, self.value, self.tb, limit=None
            )
            return tb_stack
        else:
            tb_stack = traceback.format_exception(
                self.type, self.value, self.tb, limit=-1
            )
            idx = tb_stack.index('Traceback (most recent call last):\n')
            tb_stack.insert(idx + 1, '...\n...\n')
            return tb_stack

    def message(self):
        tb_stack = self.prepare_initial_tb()
        msg = f"{RED}{''.join(tb_stack)}{RESET}"
        msg = re.sub(r'"<[a-z\s\/]+>"', self.name, msg)
        return msg


@dataclass
class ExecutionSyntaxErrorEvent(ExecutionErrorEvent):

    def message(self):
        tb_stack = self.prepare_initial_tb()
        exec_regex = r"exec\(.*\)"
        tb_stack = [
            statement for statement in tb_stack if len(re.findall(exec_regex, statement)) == 0  # noqa: E501
        ]
        msg = f"{RED}{''.join(tb_stack)}{RESET}"
        msg = re.sub(r'"<[a-z\s\/]+>"', self.name, msg)
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


@dataclass
class ServingDocsEvent(Event):
    address: str
    port: int

    def message(self):
        return f'Serving docs at {BOLD}{YELLOW}http://127.0.0.1:{self.port}{RESET}'


@dataclass
class ServingDocsExitInfo(Event):

    def message(self) -> str:
        return f'Press {BOLD}{YELLOW}Ctrl+C{RESET} to exit{RESET}'


@dataclass
class SysPathConfigWarningEvent(Event):

    def message(self):
        return f'{YELLOW}`SYS_PATH_CONF` not found in prism_project.py; adding project directory to sys.path{RESET}'  # noqa: E501


@dataclass
class ProjectDirNotInSysPath(Event):

    def message(self):
        return f'{YELLOW}project directory not in `SYS_PATH_CONF`{RESET}'


@dataclass
class ProfileDirWarningEvent(Event):

    def message(self):
        return f'{YELLOW}`PROFILES_DIR` not found in prism_project.py; defaulting to project directory{RESET}'  # noqa: E501


@dataclass
class ThreadsWarningEvent(Event):

    def message(self):
        return f'{YELLOW}`THREADS` not found in prism_project.py; defaulting to 1{RESET}'  # noqa: E501


@dataclass
class DelayEvent(Event):
    name: str
    delay_seconds: int

    def message(self):
        if self.delay_seconds > 0:
            return f'{YELLOW}{self.name} failed...delaying {self.delay_seconds} before restarting{RESET}'  # noqa: E501
        else:
            return f'{YELLOW}{self.name} failed...restarting immediately{RESET}'


@dataclass
class HeaderEvent(Event):
    msg: str

    def header_str(self):
        return self.msg

    def message(self):
        header_fix = int((TERMINAL_WIDTH - len(' ' + escape_ansi(self.header_str()) + ' ')) / 2)  # noqa: E501
        return f'{GRAY}{"=" * header_fix} {self.header_str()} {"=" * header_fix}{RESET}'


@dataclass
class TasksHeaderEvent(HeaderEvent):
    msg: str

    def header_str(self):
        return f'tasks ({GRAY_PINK}{self.msg}{GRAY})'


@dataclass
class TriggersHeaderEvent(HeaderEvent):
    msg: str = 'triggers'


@dataclass
class TriggersPathNotDefined(Event):

    def message(self):
        return f'{YELLOW}`TRIGGERS_DIR` not found in prism_project.py; defaulting to project directory{RESET}'  # noqa: E501


@dataclass
class SettingUpTriggersEvent(Event):

    def message(self):
        return 'Setting up triggers...'


@dataclass
class CreatingTasksEvent(Event):

    def message(self):
        return 'Creating tasks...'


@dataclass
class DeprecationEvent(Event):
    lineno: int
    deprecated_fn: str
    updated_fn: str

    def message(self):
        return f"{YELLOW}<line {self.lineno}>: the {self.deprecated_fn} method is deprecated, use {self.updated_fn} instead{RESET}"  # noqa: E501


def deprecated(deprecated_fn: str, updated_fn: str):
    """
    Decorator used to mark deprecated target function
    """
    def decorator_deprecated(func):

        @functools.wraps(func)
        def new_func(*args, **kwargs):

            # Suppress warning using context manager; capture line no. information
            with warnings.catch_warnings(record=True) as w:
                warnings.warn(
                    f"{YELLOW}[WARNING]: {deprecated_fn} method is deprecated, use {updated_fn} instead{RESET}",  # noqa: E501
                    category=DeprecationWarning,
                    stacklevel=2
                )

                # Iterate through warnings
                for wi in w:
                    wi = w[0]
                    lineno = wi.lineno
                    fire_console_event(
                        DeprecationEvent(
                            lineno, deprecated_fn, updated_fn
                        ),
                        log_level='warn'
                    )
            return func(*args, **kwargs)

        return new_func

    return decorator_deprecated


def fire_console_event(
    event: Optional[Event],
    event_list: List[Event] = [],
    sleep=0.01,
    log_level: str = 'info',
    formatted: bool = True
):
    """
    Fire console event. Note that if --quietly is invoked, then we set the log level
    to WARN.

    args:
        event: instance of Event class
        event_list: list of events
        sleep: number of seconds to pause after firing the event
        log_level: one of `info`, `warn`, `error`, or `critical`
    returns:
        event_list with `event` appended
    """
    if event is not None:
        if log_level == "info":
            msg = format_console_output(event.message(), log_level, formatted)
            DEFAULT_LOGGER.info(msg)  # type: ignore
        elif log_level == "warn":
            msg = format_console_output(event.message(), log_level, formatted)
            DEFAULT_LOGGER.warning(msg)  # type: ignore
        elif log_level == "error":
            msg = format_console_output(event.message(), log_level, formatted)
            DEFAULT_LOGGER.error(msg)  # type: ignore
        elif log_level == "critical":
            msg = format_console_output(event.message(), log_level, formatted)
            DEFAULT_LOGGER.critical(msg)  # type: ignore

    # Sleep
    time.sleep(sleep)

    # Return event list
    if event is not None:
        event_list.append(event)
    return event_list


def fire_empty_line_event(event_list: List[Event] = []):
    """
    Fire empty line event. These events are used to make the console logs look prettier,
    so they'll always be fired under the `info` level.
    """
    e = EmptyLineEvent()
    msg = e.message()
    DEFAULT_LOGGER.info(msg)  # type: ignore
    event_list.append(e)
    return event_list
