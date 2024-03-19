import os
from pathlib import Path
import time
from typing import Optional

import prism.logging.loggers
import prism.logging.events


def _previous_console_output():
    try:
        output_str = prism.logging.loggers.CONSOLE.file.getvalue()
        return output_str
    except AttributeError:
        return ""


def _mock_fire_console_event(
    event: Optional[prism.logging.events.Event], sleep=0.01, log_level: str = "info"
):
    if event:
        prism.logging.loggers.CONSOLE.print(event.message())
    time.sleep(sleep)


def _console_mocker(monkeypatch):
    monkeypatch.setattr(
        "prism.logging.events.fire_console_event", _mock_fire_console_event
    )


def _remove_files_in_output(wkdir):
    """
    Remove file outputs from `output` folder of project
    """
    for _file in Path(wkdir / "output").iterdir():
        if Path(wkdir / "output" / _file).is_file() and _file.name != ".exists":
            os.unlink(_file)


def _file_as_str(path):
    """
    Open file as string
    """
    with open(path, "r") as f:
        compiled_task_str = f.read()
    f.close()
    return compiled_task_str
