# Standard library imports
import json
import os
import shutil
from io import StringIO
from pathlib import Path
from unittest import mock

import prism.logging.loggers

# Prism imports
from prism.main import cli
from prism.tests.integration.integration_utils import (
    _file_as_str,
    _remove_files_in_output,
)
from prism.tests.integration.mocks import MockConsole

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TEST_PROJECTS = Path(TEST_CASE_WKDIR) / "test_projects"


# Tests
def test_init():
    with mock.patch("prism.logging.loggers.Console") as mock_console:
        mock_console.return_value = MockConsole()

        # Set up the logger
        prism.logging.loggers.set_up_logger(
            "info",
            StringIO(),
        )

        # Set working directory
        os.chdir(TEST_PROJECTS)

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


def test_run_normal():
    with mock.patch("prism.logging.loggers.Console") as mock_console:
        mock_console.return_value = MockConsole()

        wkdir = TEST_PROJECTS / "005_simple_project_no_null_tasks"

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


def test_run_callbacks():
    with mock.patch("prism.logging.loggers.Console") as mock_console:
        with mock.patch(
            "prism.logging.events.fire_console_event"
        ) as mock_fire_console_event:
            import re

            mock_console.return_value = MockConsole()
            mock_fire_console_event.side_effect = (
                lambda x,
                sleep=0.01,
                log_level="info": mock_console.return_value.messages.append(
                    re.sub(r"\[[\/a-z\s]+\]", "", x.message())
                )
            )

            wkdir = TEST_PROJECTS / "005_simple_project_no_null_tasks"

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
            for i in range(1, 5):
                assert any(
                    [
                        f"RUNNING TASK module0{i}.Task0{i}" in m
                        for m in mock_console.return_value.messages
                    ]
                )
                assert any(
                    [
                        f"FINISHED TASK module0{i}.Task0{i}" in m
                        for m in mock_console.return_value.messages
                    ]
                )
            assert any(
                [
                    "Running on_success callbacks" in m
                    for m in mock_console.return_value.messages
                ]
            )
            assert any(
                [
                    "FINISHED running print_success callback" in m
                    for m in mock_console.return_value.messages
                ]
            )


def test_run_connectors():
    with mock.patch("prism.logging.loggers.Console") as mock_console:
        mock_console.return_value = MockConsole()

        wkdir = TEST_PROJECTS / "013_connectors"

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
            "--connector",
            "additional_package.cli_connectors.snowflake_connector",
            "--disable-rich-logging",
            "--runtime-ctx",
            json.dumps({"OUTPUT": str(output_dir)}),
        ]
        _ = cli(args, standalone_mode=False)

        # Check output
        assert (wkdir / "output" / "machinery_sample.csv").is_file()
        assert (wkdir / "output" / "household_sample.csv").is_file()
        _remove_files_in_output(wkdir)
