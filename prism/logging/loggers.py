import logging
import re
import sys
from io import StringIO
from logging import Formatter, Handler, StreamHandler
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Literal, Optional, Union

from rich.console import Console
from rich.logging import RichHandler
from rich.theme import Theme

CONSOLE: Console
DEFAULT_LOGGER: logging.Logger


LOGFORMAT = "%(asctime)s | %(message)s"
LOGFORMAT_RICH = "| %(message)s"


def escape_rich_formatting(string: str) -> str:
    """
    Replace Rich formatting e.g., [blue]...[/blue]. This cleans up the logs we save to a
    file.
    """
    return re.sub(r"\[/?[a-z]+\]", "", string)


def console_print(msg: Union[List[str], str], **kwargs) -> None:
    """
    Thin wrapper around `console.print(...)` in order to add the printed messages to our
    logs.
    """
    try:
        fh = DEFAULT_LOGGER.handlers[0]
    except IndexError:
        if not isinstance(msg, str):
            CONSOLE.print("\n" + "".join(msg), **kwargs)
        return None

    # For tracebacks
    if not isinstance(msg, str):
        msg = "\n" + "".join(msg)
        record = logging.LogRecord(
            name="",
            level=40,
            pathname="",
            lineno=0,
            msg=msg,
            args=None,
            exc_info=None,
        )
        fh.emit(record)

    else:
        CONSOLE.print(msg, **kwargs)
        msg_no_formatting = escape_rich_formatting(msg)

        # If the message is a header / tail rule, then ignore
        if len(re.findall(r"^\─+$", msg_no_formatting)):
            return None

        # Silently log the message. We only silently log `info` messages.
        record = logging.LogRecord(
            name="",
            level=20,
            pathname="",
            lineno=0,
            msg=msg_no_formatting,
            args=None,
            exc_info=None,
        )
        fh.emit(record)
    return None


class FileHandlerFormatter(logging.Formatter):
    def format(self, record):
        if re.findall(r"^\s+$", record.msg) or record.msg == "":
            formatter = logging.Formatter("%(message)s", "%H:%M:%S")
        else:
            formatter = logging.Formatter(LOGFORMAT, "%H:%M:%S")
        return escape_rich_formatting(formatter.format(record))


def set_up_logger(
    log_level: Literal["info", "warning", "error", "debug", "critical"],
    fpath: Optional[Union[str, Path, StringIO]],
    rich_logging: bool = True,
):
    if globals().get("DEFAULT_LOGGER", None) is None:
        global CONSOLE
        global DEFAULT_LOGGER

        # Instantiate Console
        CONSOLE = Console(
            highlight=False,
            theme=Theme(
                {
                    "logging.level.info": "cyan",
                    "logging.level.warning": "yellow",
                    "logging.level.error": "red",
                    "logging.level.debug": "orange1",
                }
            ),
            file=fpath if isinstance(fpath, StringIO) else sys.stdout,
        )

        # Instantiate Rich handler
        handlers: List[Handler] = []
        if rich_logging:
            rh = RichHandler(
                rich_tracebacks=True,
                tracebacks_width=120,
                show_path=False,
                omit_repeated_times=False,
                console=CONSOLE,
                markup=True,
                log_time_format="[%X]",
                highlighter=None,
            )
            rh.setFormatter(Formatter(LOGFORMAT_RICH))
            handlers.append(rh)
        else:
            sh = StreamHandler(sys.stdout)
            sh.setFormatter(FileHandlerFormatter())
            handlers.append(sh)

        # We also want to save our logs on disk, unless the inputted file is a StringIO
        # class (used in our tests).
        if fpath and not isinstance(fpath, StringIO):
            file_handler = RotatingFileHandler(
                filename=fpath,
                maxBytes=1024 * 1024 * 10,
                backupCount=10,  # 10Mb
            )
            file_handler.setFormatter(FileHandlerFormatter())
            handlers.append(file_handler)

        logging.basicConfig(
            level=log_level.upper(),
            format=LOGFORMAT,
            handlers=handlers,
        )
        DEFAULT_LOGGER = logging.getLogger("prism")
        if fpath and not isinstance(fpath, StringIO):
            DEFAULT_LOGGER.addHandler(file_handler)
