# General package imports
import re
import os
import copy
import math
import time
from dataclasses import dataclass
from typing import Literal, Optional
from pathlib import Path

# Prism imports
import prism.constants
import prism.exceptions
import prism.logging.loggers


# Terminal width
TERMINAL_WIDTH = 80


# Util functions
def colorize_status(status):
    """
    Colorize status; RUN should be gray, DONE should be green, ERROR should be red

    args:
        status: status (either RUN, DONE, ERROR)
    returns
        colorized_status: status with color
    """
    if status not in ["RUN", "DONE", "ERROR", "SKIP"]:
        raise ValueError(
            f"{status} is invalid; must be either RUN, DONE, ERROR, or SKIP"
        )  # noqa
    if status == "RUN":
        return "[yellow]RUN[/yellow]"
    elif status == "DONE":
        return "[green]DONE[/green]"
    elif status == "ERROR":
        return "[red]ERROR[/red]"
    elif status == "SKIP":
        return "[orange1]SKIP[orange1]"


def escape_ansi(string: str) -> str:
    """
    Replace ANSI escape codes with ''

    args:
        string: string containing ANSI codes
    returns:
        inputted string without ANSI codes
    """
    ansi_regex = re.compile("\x1b[^m]*m")
    return ansi_regex.sub("", string)


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
    ansi_regex = r"\x1b[^m]*m"
    rich_markup_regex = r"\[[\/a-z\s]+\]"

    # ANSI matches
    ansi_matches = [(m.start(), m.end()) for m in re.finditer(ansi_regex, string)]
    markup_matches = [
        (m.start(), m.end()) for m in re.finditer(rich_markup_regex, string)
    ]  # noqa: E501

    # Replace ANSI matches with ''
    string_with_ansi_replaced = re.sub(ansi_regex, "", string)
    string_with_markup_replaced = re.sub(
        rich_markup_regex, "", string_with_ansi_replaced
    )  # noqa: E501

    # ljust
    num_chars_to_add = width - len(string_with_markup_replaced)
    ljust_string = char * num_chars_to_add
    string_ljust = string_with_markup_replaced + "[dim]" + ljust_string + "[/dim]"

    # Add ANSI characters back in
    string_ljust_with_styling = copy.deepcopy(string_ljust)
    for match in ansi_matches + markup_matches:
        start = match[0]
        end = match[1]
        string_ljust_with_styling = (
            string_ljust_with_styling[:start]
            + string[start:end]  # noqa: W503
            + string_ljust_with_styling[start:]  # noqa: W503
        )
    return string_ljust_with_styling


# Event classes
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
class CreatingPrismProjectTemplate(Event):
    project_dir: Path

    def message(self):
        return f"[bold]Creating template project at [magenta]{self.project_dir}[/magenta][/bold]..."  # noqa: E501


@dataclass
class RunningWithPrismEvent(Event):

    def message(self):
        return f"[bold]Running with Prism [magenta]v{prism.constants.VERSION}[/magenta]..."  # noqa: E501


@dataclass
class CreatingRunEvent(Event):
    run_slug: str
    project_id: str

    def message(self):
        return f"[bold]Creating run [magenta]{self.run_slug}[/magenta] for client [magenta]{self.project_id}[/magenta]..."  # noqa: E501


@dataclass
class FoundTasksModulesEvent(Event):
    num_tasks: int
    num_modules: int
    run_slug: Optional[str]

    def message(self):
        if self.run_slug:
            return f"[bold]Found [magenta]{self.num_tasks}[/magenta] task(s) in [magenta]{self.num_modules}[/magenta] module(s) in job [magenta]{self.run_slug}[/magenta]..."  # noqa: E501
        else:
            return f"[bold]Found [magenta]{self.num_tasks}[/magenta] task(s) in [magenta]{self.num_modules}[/magenta] module(s)..."  # noqa: E501


@dataclass
class SectionEvent(Event):
    section_title: str

    def message(self):
        len_title = len(f" {self.section_title} ")
        rule_length = int((100 - len_title) / 2)
        rule = "─" * rule_length
        return "[dim]" + rule + f" {self.section_title} " + rule + "[/dim]"


@dataclass
class RunStatusEvent(Event):
    run_slug: str
    status: Literal["succeeded", "failed"]

    def message(self):
        return f"[bold]Run [magenta]{self.run_slug}[/magenta] {self.status}...[/bold]"


@dataclass
class SuccessCallbackEvent(Event):

    def message(self):
        return "[bold]Running [green]on_success[/green] callbacks...[/bold]"


@dataclass
class FailureCallbackEvent(Event):

    def message(self):
        return "[bold]Running [red]on_failure[/red] callbacks...[/bold]"


@dataclass
class CurrentProjectDirEvent(Event):
    path: str

    def message(self):
        return f"[bold]Found project directory at [magenta]{self.path}[/magenta][/bold]"


@dataclass
class SettingUpProfileEvent(Event):

    def message(self):
        return "Setting up profile..."


@dataclass
class InitSuccessfulEvent(Event):
    msg: str

    def message(self):
        return self.msg


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
            RUNNING EVENT {event.name}
            FINISHED EVENT {event.name}
            ERROR IN EVENT {event.name}
            SKIPPED EVENT {event.name}

        Add name of event (after removing event status and ANSI codes) to string
        representation of event.
        """
        # Remove all ANSI codes
        msg_no_ansi = escape_ansi(self.msg)

        # Remove the event status
        status_regex = re.compile("(RUNNING|FINISHED|ERROR|SKIPPING)")
        msg_no_ansi_status = status_regex.sub("", msg_no_ansi)

        # Remove EVENT and quotation marks
        event_name_str = (
            msg_no_ansi_status.replace("IN EVENT", "")
            .replace("EVENT", "")
            .replace("'", "")
        )

        # Remove all trailing / leading spaces
        return super().__str__() + " - " + event_name_str.strip() + " - " + self.status

    def message(self):
        message = self.msg
        if self.num is not None and self.total is not None:
            message = f"{self.num} of {self.total} {self.msg}"

        # Add execution time and status
        try:
            width = min(os.get_terminal_size(0)[0], TERMINAL_WIDTH)
        except Exception:
            width = TERMINAL_WIDTH
        truncate_width = math.ceil(0.9 * (width))
        justified = custom_ljust(message, truncate_width, ".")

        if self.execution_time is None:
            status_time = ""
        else:
            status_time = f" in {self.execution_time:0.2f}s"
        colorized_status = colorize_status(self.status)
        output = f"{justified} [{colorized_status}{status_time}]"
        return output


@dataclass
class ServingDocsEvent(Event):
    address: str
    port: int

    def message(self):
        return f"Serving docs at [bold yellow]http://127.0.0.1:{self.port}[/bold yellow]"  # noqa: E501


@dataclass
class ServingDocsExitInfo(Event):

    def message(self) -> str:
        return "Press [bold yellow]Ctrl+C[/bold yellow] to exit"


@dataclass
class DelayEvent(Event):
    name: str
    delay_seconds: int

    def message(self):
        if self.delay_seconds > 0:
            return f"[yellow]{self.name} failed...delaying {self.delay_seconds} before restarting[/yellow]"  # noqa: E501
        else:
            return f"[yellow]{self.name} failed...restarting immediately[/yellow]"


@dataclass
class DocsReloadedEvent(Event):

    def message(self):
        return "Project has been re-compiled. Press [bold yellow]Cmd+Shift+R[/bold yellow] to reload your graph"  # noqa: E501


def fire_console_event(
    event: Optional[Event], sleep=0.01, log_level: str = "info"
) -> None:
    """
    Fire console event. Note that if --quietly is invoked, then we set the log level
    to WARN.

    args:
        event: instance of Event class
        sleep: number of seconds to pause after firing the event
        log_level: one of `info`, `warn`, `error`, or `debug`. Default is `info`
    returns:
        None
    """
    if event is not None:
        if log_level == "info":
            prism.logging.loggers.DEFAULT_LOGGER.info(
                event.message(), extra={"highlighter": None}
            )
        elif log_level == "warning":
            prism.logging.loggers.DEFAULT_LOGGER.warning(
                event.message(), extra={"highlighter": None}
            )
        elif log_level == "error":
            prism.logging.loggers.DEFAULT_LOGGER.error(
                event.message(), extra={"highlighter": None}
            )
        elif log_level == "debug":
            prism.logging.loggers.DEFAULT_LOGGER.debug(
                event.message(), extra={"highlighter": None}
            )
        elif log_level == "critical":
            prism.logging.loggers.DEFAULT_LOGGER.critical(
                event.message(), extra={"highlighter": None}
            )

    # Sleep
    time.sleep(sleep)


def fire_init_events():
    # These are super similar to the header events, but we don't want to log them, we
    # want to print them.
    prism.logging.loggers.console_print("[bold]" + "━" * 100 + "[/bold]")
    prism.logging.loggers.console_print(RunningWithPrismEvent().message())


def fire_empty_line_event(n: int = 1) -> None:
    """
    Fire an empty line in the console
    """
    if prism.logging.loggers.DEFAULT_LOGGER.level <= 20:
        for _ in range(n):
            prism.logging.loggers.console_print("")


def fire_header_events(
    project_id: Optional[str],
    run_slug: Optional[str],
    num_modules: Optional[int],
    num_tasks: Optional[int],
) -> None:
    """
    Fire an empty line in the console
    """
    prism.logging.loggers.console_print("[bold]" + "━" * 100 + "[/bold]")
    fire_console_event(RunningWithPrismEvent())
    if project_id and run_slug:
        fire_console_event(CreatingRunEvent(run_slug=run_slug, project_id=project_id))
    if num_modules is not None and num_tasks is not None:
        fire_console_event(
            FoundTasksModulesEvent(
                num_tasks=num_tasks, num_modules=num_modules, run_slug=run_slug
            )
        )
    fire_empty_line_event()


def fire_section_event(section_title: str):
    fire_empty_line_event()
    prism.logging.loggers.console_print(
        SectionEvent(section_title=section_title).message()
    )


def fire_serving_docs_events(address: str, port: int):
    fire_empty_line_event()
    fire_console_event(ServingDocsEvent(address=address, port=port), log_level="info")
    fire_console_event(ServingDocsExitInfo(), log_level="info")
    fire_empty_line_event()


def fire_reload_docs_event():
    fire_empty_line_event()
    fire_console_event(DocsReloadedEvent())
    fire_empty_line_event()


def fire_callback_events(
    run_slug: str, callback_type: Literal["on_success", "on_failure"]
):
    if callback_type == "on_success":
        fire_console_event(RunStatusEvent(run_slug=run_slug, status="succeeded"))
        fire_console_event(SuccessCallbackEvent())
    else:
        fire_console_event(RunStatusEvent(run_slug=run_slug, status="failed"))
        fire_console_event(FailureCallbackEvent())


def fire_tail_events() -> None:
    """
    Fire an empty line in the console
    """
    fire_empty_line_event()
    prism.logging.loggers.console_print("[bold green]Done![/bold green]")
    prism.logging.loggers.console_print("[bold]" + "━" * 100 + "[/bold]")
