# Standard library imports
from io import StringIO
import json
import os
from pathlib import Path
import shutil

# Prism imports
from prism.main import cli
import prism.logging.loggers
from prism.tests.integration.integration_utils import (
    _previous_console_output,
    _remove_files_in_output,
    _console_mocker,
    _file_as_str,
)


# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TEST_PROJECTS = Path(TEST_CASE_WKDIR) / "test_projects"


# Tests
def test_init(monkeypatch):
    # Set up the logger
    prism.logging.loggers.set_up_logger(
        "info",
        StringIO(),
    )

    # Set working directory
    os.chdir(TEST_PROJECTS)
    _console_mocker(monkeypatch)

    # Remove folder '001_init' if it already exists
    init_path = Path(TEST_PROJECTS) / "001_init"
    if init_path.is_dir():
        shutil.rmtree(init_path)

    # Execute the CLI command
    args = ["init", "--project-name", "001_init"]
    _ = cli(args, standalone_mode=False)
    assert init_path.is_dir()

    # Change to the parent directory
    os.chdir(TEST_PROJECTS.parent)


def test_run_normal(monkeypatch):
    wkdir = TEST_PROJECTS / "005_simple_project_no_null_tasks"
    _console_mocker(monkeypatch)

    # Remove output
    _remove_files_in_output(wkdir)
    assert not Path(wkdir / "output" / "task01.txt").is_file()
    assert not Path(wkdir / "output" / "task02.txt").is_file()

    # Execute the CLI command
    output_dir = wkdir / "output"
    args = [
        "run",
        "--tasks-dir",
        str(wkdir / "tasks"),
        "--disable-rich-logging",
        "--runtime-ctx",
        json.dumps({"OUTPUT": str(output_dir)}),
    ]
    _ = cli(args, standalone_mode=False)

    # Check outputs
    task01_txt = _file_as_str(Path(wkdir / "output" / "task01.txt"))
    task02_txt = _file_as_str(Path(wkdir / "output" / "task02.txt"))
    task01_txt = _file_as_str(Path(wkdir / "output" / "task01.txt"))
    task02_txt = _file_as_str(Path(wkdir / "output" / "task02.txt"))
    assert "Hello from task 1!" == task01_txt
    assert "Hello from task 1!" + "\n" + "Hello from task 2!" == task02_txt


def test_run_callbacks(monkeypatch):
    wkdir = TEST_PROJECTS / "005_simple_project_no_null_tasks"
    _console_mocker(monkeypatch)

    # Previous output
    prev_console_output = _previous_console_output()

    # Remove output
    _remove_files_in_output(wkdir)
    assert not Path(wkdir / "output" / "task01.txt").is_file()
    assert not Path(wkdir / "output" / "task02.txt").is_file()

    # Execute the CLI command
    output_dir = wkdir / "output"
    args = [
        "run",
        "--tasks-dir",
        str(wkdir / "tasks"),
        "--on-success",
        "additional_package.cli_callbacks.print_success",
        "--disable-rich-logging",
        "--runtime-ctx",
        json.dumps({"OUTPUT": str(output_dir)}),
    ]
    _ = cli(args, standalone_mode=False)

    # Check outputs
    task01_txt = _file_as_str(Path(wkdir / "output" / "task01.txt"))
    task02_txt = _file_as_str(Path(wkdir / "output" / "task02.txt"))
    task01_txt = _file_as_str(Path(wkdir / "output" / "task01.txt"))
    task02_txt = _file_as_str(Path(wkdir / "output" / "task02.txt"))
    assert "Hello from task 1!" == task01_txt
    assert "Hello from task 1!" + "\n" + "Hello from task 2!" == task02_txt

    # Logs
    output_str = prism.logging.loggers.CONSOLE.file.getvalue()  # type: ignore
    output_str = output_str.replace(prev_console_output, "")
    for i in range(1, 5):
        assert f"RUNNING TASK module0{i}.Task0{i}" in output_str
        assert f"FINISHED TASK module0{i}.Task0{i}" in output_str
    assert "Running on_success callbacks" in output_str
    assert "FINISHED running print_success callback" in output_str


def test_run_connectors(monkeypatch):
    wkdir = TEST_PROJECTS / "013_connectors"
    _console_mocker(monkeypatch)

    # Remove output
    _remove_files_in_output(wkdir)

    # Execute the CLI command
    output_dir = wkdir / "output"
    args = [
        "run",
        "--tasks-dir",
        str(wkdir / "tasks"),
        "--task",
        "snowflake_task.SnowflakeTask",
        "--task",
        "postgres_task.PostgresTask",
        "--connector",
        "additional_package.cli_connectors.snowflake_connector",
        "--connector",
        "additional_package.cli_connectors.postgres_connector",
        "--disable-rich-logging",
        "--runtime-ctx",
        json.dumps({"OUTPUT": str(output_dir)}),
    ]
    _ = cli(args, standalone_mode=False)

    # Check output
    assert (wkdir / "output" / "sample_postgres_data.csv").is_file()
    assert (wkdir / "output" / "machinery_sample.csv").is_file()
    assert (wkdir / "output" / "household_sample.csv").is_file()
    _remove_files_in_output(wkdir)
