# Standard library imports
import os
import re
from io import StringIO
from pathlib import Path
from typing import Literal
from unittest import mock

import pandas as pd
import pytest

# SQLAlchemy
from sqlalchemy import select

# Prism imports
import prism.exceptions
import prism.logging.events
import prism.logging.loggers
from prism.client import PrismProject
from prism.db.factory import ThreadLocalSessionFactory
from prism.db.setup import Project, Ref
from prism.tests.integration.integration_utils import (
    _file_as_str,
    _remove_files_in_output,
)
from prism.tests.integration.mocks import MockConsole

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TEST_PROJECTS = Path(TEST_CASE_WKDIR) / "test_projects"


def _validate_simple_project_log_output(messages: list[str]):
    num_tasks = 4
    for i in range(1, 5):
        assert any(
            [
                f"{i} of {num_tasks} RUNNING TASK module0{i}.Task0{i}" in m
                for m in messages
            ]
        )
        assert any(
            [
                f"{i} of {num_tasks} FINISHED TASK module0{i}.Task0{i}" in m
                for m in messages
            ]
        )


@pytest.fixture(scope="module")
def mock_console():
    with mock.patch("prism.logging.loggers.Console") as mock_console:
        with mock.patch(
            "prism.logging.events.fire_console_event"
        ) as mock_fire_console_event:
            mock_console.return_value = MockConsole()
            mock_fire_console_event.side_effect = (
                lambda x,
                sleep=0.01,
                log_level="info": mock_console.return_value.messages.append(
                    re.sub(r"\[[\/a-z0-9\s]+\]", "", x.message())
                )
            )
            yield mock_console


# Tests
def test_simple_project_all_tasks(mock_console):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = Path(TEST_PROJECTS) / "004_simple_project"
    os.chdir(wkdir)

    # Create client and run
    client = PrismProject(
        id="simple-project",
        tasks_dir=wkdir / "modules",
    )
    with pytest.raises(prism.exceptions.RuntimeException) as cm:
        client.run(
            runtime_ctx={
                "OUTPUT": wkdir / "output",
            },
            rich_logging=False,
            log_file=StringIO(),
        )
    expected_msg = "`run` method must produce a non-null output"
    assert str(cm.value) == expected_msg

    # Check logs / other events
    assert any(["Running with Prism" in m for m in mock_console.return_value.messages])
    assert any(
        [
            "Found 3 task(s) in 3 module(s)" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        ["Parsing task dependencie" in m for m in mock_console.return_value.messages]
    )
    assert any(
        [
            "FINISHED parsing task dependencie" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "1 of 3 RUNNING TASK module03.Task03" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "1 of 3 ERROR IN TASK module03.Task03" in m
            for m in mock_console.return_value.messages
        ]
    )


def test_simple_project_no_null_all_tasks(mock_console):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = Path(TEST_PROJECTS) / "005_simple_project_no_null_tasks"
    os.chdir(wkdir)

    # Remove all files in the output directory
    _remove_files_in_output(wkdir)

    # Create client and run
    client = PrismProject(
        id="simple-project-no-null-output",
        tasks_dir=wkdir / "tasks",
    )
    client.run(
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        rich_logging=False,
        log_file=StringIO(),
    )

    # Check outputs
    task01_txt = _file_as_str(Path(wkdir / "output" / "task01.txt"))
    task02_txt = _file_as_str(Path(wkdir / "output" / "task02.txt"))
    assert "Hello from task 1!" == task01_txt
    assert "Hello from task 1!" + "\n" + "Hello from task 2!" == task02_txt

    # Check logs / other events
    _validate_simple_project_log_output(mock_console.return_value.messages)


def test_database(mock_console):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = Path(TEST_PROJECTS) / "005_simple_project_no_null_tasks"
    os.chdir(wkdir)

    # Remove all files in the output directory
    _remove_files_in_output(wkdir)

    # Create client and run
    client = PrismProject(
        id="simple-project-db-test",
        tasks_dir=wkdir / "tasks",
    )
    client.run(
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        rich_logging=False,
        log_file=StringIO(),
    )

    # Refs in database
    factory = ThreadLocalSessionFactory()
    with factory.create_thread_local_session() as session:
        project_stmt = select(Project).where(Project.local_path == str(wkdir))
        res = factory.execute_thread_local_stmt(project_stmt, session)
        project_id = res[0].id

        ref_stmt = select(Ref).where(Ref.project_id == project_id)
        res = factory.execute_thread_local_stmt(ref_stmt, session)
    assert len(res) == 3
    refs = []
    for r in res:
        refs.append((r.source_id, r.target_id))
    assert ("module01.Task01", "module02.Task02") in refs
    assert ("module02.Task02", "module03.Task03") in refs
    assert ("module03.Task03", "module04.Task04") in refs


def test_simple_project_no_null_subset(mock_console):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = Path(TEST_PROJECTS) / "005_simple_project_no_null_tasks"
    os.chdir(wkdir)

    # Remove all files in the output directory
    _remove_files_in_output(wkdir)

    # *************** #
    # Run only task 1 #
    # *************** #

    # Create client and run. Expecatation: task 1 is the first task in the DAG.
    # Therefore, we should not encounter any errors with this command.
    client = PrismProject(
        id="simple-project-no-null-output",
        tasks_dir=wkdir / "tasks",
    )
    client.run(
        task_ids=["module01.Task01"],
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        rich_logging=False,
        log_file=StringIO(),
    )

    # Check outputs
    task01_txt = _file_as_str(Path(wkdir / "output" / "task01.txt"))
    assert "Hello from task 1!" == task01_txt
    assert not (Path(wkdir / "output" / "task02.txt")).is_file()

    # Check logs / other events
    assert any(
        [
            "1 of 1 RUNNING TASK module01.Task01" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "1 of 1 FINISHED TASK module01.Task01" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "RUNNING TASK module02.Task02" not in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "FINISHED TASK module02.Task02" not in m
            for m in mock_console.return_value.messages
        ]
    )

    # ************** #
    # Execute task 2 #
    # ************** #
    # Reset the messages
    mock_console.return_value.messages = []

    # Create client and run. Expecatation: task 2 depends on task 1. However, since we
    # just ran task 1, and the output of task 1 is stored in a target, we do not need to
    # re-run task 1 in order to run task 2. Therefore, we should not encounter any
    # errors with this command.
    client = PrismProject(
        id="simple-project-no-null-output",
        tasks_dir=wkdir / "tasks",
    )
    client.run(
        task_ids=["module02.Task02"],
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        rich_logging=False,
        log_file=StringIO(),
    )

    # Check outputs
    assert (Path(wkdir / "output" / "task02.txt")).is_file()
    task02_txt = _file_as_str(Path(wkdir / "output" / "task02.txt"))
    assert "Hello from task 1!" + "\n" + "Hello from task 2!" == task02_txt

    # Check logs / other events
    assert any(
        [
            "1 of 1 RUNNING TASK module02.Task02" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "1 of 1 FINISHED TASK module02.Task02" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "RUNNING TASK module01.Task01" not in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "FINISHED TASK module01.Task01" not in m
            for m in mock_console.return_value.messages
        ]
    )

    # *********************************************** #
    # Execute task 4 (with and without `all-upstream` #
    # *********************************************** #
    # Reset the messages
    mock_console.return_value.messages = []

    # Create client and run. Expectation: task 4 depends on task 3. However, the output
    # of task 3 is not stored in a target. Therefore, running task 4 without including
    # 'all-upstream' should cause an error.
    client = PrismProject(
        id="simple-project-no-null-output",
        tasks_dir=wkdir / "tasks",
    )
    with pytest.raises(prism.exceptions.RuntimeException) as cm:
        client.run(
            task_ids=["module04.Task04"],
            all_tasks_upstream=False,
            runtime_ctx={
                "OUTPUT": wkdir / "output",
            },
            rich_logging=False,
            log_file=StringIO(),
        )
    expected_msg = "cannot access the output of `Task03` without either explicitly running task or setting a target"  # noqa: E501
    assert str(cm.value) == expected_msg

    # Execute command with `all-upstream`
    mock_console.return_value.messages = []
    client.run(
        task_ids=["module04.Task04"],
        all_tasks_upstream=True,
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        rich_logging=False,
        log_file=StringIO(),
    )

    # Check outputs
    task01_txt = _file_as_str(Path(wkdir / "output" / "task01.txt"))
    task02_txt = _file_as_str(Path(wkdir / "output" / "task02.txt"))
    assert "Hello from task 1!" == task01_txt
    assert "Hello from task 1!" + "\n" + "Hello from task 2!" == task02_txt

    # Check logs / other events
    _validate_simple_project_log_output(mock_console.return_value.messages)


def test_project_nested_task_dirs(mock_console):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = Path(TEST_PROJECTS) / "010_project_nested_module_dirs"
    os.chdir(wkdir)

    # Remove all files in the output directory
    _remove_files_in_output(wkdir)

    # Helper function
    def check_tasks_1_2_results():
        """
        Helper function to check the results of running tasks 1 and 2. We will use
        this a couple of times.
        """
        # Check that outputs are created
        assert Path(wkdir / "output" / "task01.txt").is_file()
        assert Path(wkdir / "output" / "task02.txt").is_file()
        with open(Path(wkdir / "output" / "task02.txt"), "r") as f:
            task02_txt = f.read()
        expected_content = "Hello from task 1!" + "\n" + "Hello from task 2!"
        assert task02_txt == expected_content

    # **************************************************** #
    # Execute all tasks in extract folder using '*' syntax #
    # **************************************************** #
    # Create project
    client = PrismProject(
        id="project-with-nested-directories",
        tasks_dir=wkdir / "tasks",
    )
    client.run(
        task_ids=["extract/*"],
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        rich_logging=False,
        log_file=StringIO(),
    )

    # Check results
    check_tasks_1_2_results()

    # Check logs
    assert any(
        [
            "RUNNING TASK extract/module01.Task01" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "FINISHED TASK extract/module01.Task01" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "RUNNING TASK extract/module02.Task02" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "FINISHED TASK extract/module02.Task02" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "RUNNING TASK load/module03.Task03" not in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "FINISHED TASK load/module03.Task03" not in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "RUNNING TASK module04.Task04" not in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "FINISHED TASK module04.Task04" not in m
            for m in mock_console.return_value.messages
        ]
    )

    # Remove all files in the compiled and output directory
    _remove_files_in_output(wkdir)

    # *************************************************************** #
    # Execute all tasks in extract /load folder using explicit syntax #
    # *************************************************************** #
    # Reset the messages
    mock_console.return_value.messages = []

    # Create project
    client = PrismProject(
        id="project-with-nested-directories",
        tasks_dir=wkdir / "tasks",
    )
    client.run(
        task_ids=[
            "extract/module01.Task01",
            "extract/module02.Task02",
            "load/module03.Task03",
        ],
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        rich_logging=False,
        log_file=StringIO(),
    )

    # Check results
    check_tasks_1_2_results()

    # Check logs
    assert any(
        [
            "RUNNING TASK extract/module01.Task01" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "FINISHED TASK extract/module01.Task01" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "RUNNING TASK extract/module02.Task02" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "FINISHED TASK extract/module02.Task02" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "RUNNING TASK load/module03.Task03" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "FINISHED TASK load/module03.Task03" in m
            for m in mock_console.return_value.messages
        ]
    )

    # Remove all files in the compiled and output directory
    _remove_files_in_output(wkdir)

    # ***************** #
    # Execute all tasks #
    # ***************** #
    # Reset the messages
    mock_console.return_value.messages = []

    # Create project
    client = PrismProject(
        id="project-with-nested-directories",
        tasks_dir=wkdir / "tasks",
    )
    client.run(
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        rich_logging=False,
        log_file=StringIO(),
    )

    # Check output of tasks 1 and 2
    check_tasks_1_2_results()


def test_bad_task_ref(mock_console):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = Path(TEST_PROJECTS) / "011_bad_task_ref"
    os.chdir(wkdir)

    # Create project
    client = PrismProject(
        id="project-with-bad-ref",
        tasks_dir=wkdir / "modules",
    )
    with pytest.raises(prism.exceptions.CompileException) as cm:
        client.run(
            runtime_ctx={
                "OUTPUT": wkdir / "output",
            },
            rich_logging=False,
            log_file=StringIO(),
        )
    expected_msg = "task `extract/this_is_an_error` not found in project"
    assert str(cm.value) == expected_msg


def test_concurrency(mock_console):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = Path(TEST_PROJECTS) / "012_concurrency"
    os.chdir(wkdir)

    # Remove files in output
    _remove_files_in_output(wkdir)

    # Create project
    client = PrismProject(
        id="project-with-concurrency",
        concurrency=2,
        tasks_dir=wkdir / "modules",
    )
    client.run(
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        rich_logging=False,
        log_file=StringIO(),
    )

    # Get times
    task2_times = pd.read_csv(wkdir / "output" / "task02.csv")
    task1_times = pd.read_csv(wkdir / "output" / "task01.csv")

    # Task 1 and 2 should start at the same time
    task2_start_time = int(task2_times["start_time"][0])
    task1_start_time = int(task1_times["start_time"][0])
    assert abs(task2_start_time - task1_start_time) <= 1

    # Task 2 should finish before task 1
    task2_end_time = int(task2_times["end_time"][0])
    task1_end_time = int(task1_times["end_time"][0])
    assert task2_end_time < task1_end_time
    assert abs(10 - (task1_end_time - task2_end_time)) <= 1

    # Remove files in output
    _remove_files_in_output(wkdir)


def test_runtime_ctx_overrides_client_ctx(mock_console):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = Path(TEST_PROJECTS) / "005_simple_project_no_null_tasks"
    os.chdir(wkdir)

    # Remove files in output folder
    _remove_files_in_output(wkdir)

    # Create project and run (without a run context)
    client = PrismProject(
        id="project-overrides-test",
        concurrency=2,
        tasks_dir=wkdir / "tasks",
        ctx={"OUTPUT": wkdir},
    )
    client.run(
        rich_logging=False,
        log_file=StringIO(),
    )

    # Check outputs
    assert not (Path(wkdir / "output" / "task01.txt")).is_file()
    assert not (Path(wkdir / "output" / "task02.txt")).is_file()
    task01_txt = _file_as_str(Path(wkdir / "task01.txt"))
    task02_txt = _file_as_str(Path(wkdir / "task02.txt"))
    assert "Hello from task 1!" == task01_txt
    assert "Hello from task 1!" + "\n" + "Hello from task 2!" == task02_txt
    os.unlink(Path(wkdir / "task01.txt"))
    os.unlink(Path(wkdir / "task02.txt"))

    # Now run (with the run context)
    client.run(
        runtime_ctx={"OUTPUT": wkdir / "output"},
        rich_logging=False,
        log_file=StringIO(),
    )
    assert not (Path(wkdir / "task01.txt")).is_file()
    assert not (Path(wkdir / "task02.txt")).is_file()
    task01_txt = _file_as_str(Path(wkdir / "output" / "task01.txt"))
    task02_txt = _file_as_str(Path(wkdir / "output" / "task02.txt"))
    assert "Hello from task 1!" == task01_txt
    assert "Hello from task 1!" + "\n" + "Hello from task 2!" == task02_txt


def test_all_downstream(mock_console):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = Path(TEST_PROJECTS) / "005_simple_project_no_null_tasks"
    os.chdir(wkdir)

    # Remove files in output folder
    _remove_files_in_output(wkdir)

    # Create project and run (without a run context)
    client = PrismProject(
        id="project-overrides-test",
        concurrency=2,
        tasks_dir=wkdir / "tasks",
    )
    client.run(
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        task_ids=["module01.Task01"],
        all_tasks_downstream=True,
        rich_logging=False,
        log_file=StringIO(),
    )

    # Check that outputs are created
    assert Path(wkdir / "output" / "task01.txt").is_file()
    assert Path(wkdir / "output" / "task02.txt").is_file()

    # Check outputs
    task01_txt = _file_as_str(Path(wkdir / "output" / "task01.txt"))
    task02_txt = _file_as_str(Path(wkdir / "output" / "task02.txt"))
    assert "Hello from task 1!" == task01_txt
    assert "Hello from task 1!" + "\n" + "Hello from task 2!" == task02_txt

    # Check logs / other events
    _validate_simple_project_log_output(mock_console.return_value.messages)


def helper_for_testing_callbacks(
    mock_console,
    project_dir: Path,
    tasks_dir: Path,
    callback_type: Literal["success", "failure"],
    in_client: bool = True,
):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = project_dir
    os.chdir(wkdir)

    def callback_fn():
        with open(wkdir / f"{callback_type}_callback.txt", "w") as f:
            f.write(f"This was a {callback_type}!")

    # Remove files in output folder
    _remove_files_in_output(wkdir)
    if (wkdir / f"{callback_type}_callback.txt").is_file():
        os.unlink(wkdir / f"{callback_type}_callback.txt")

    # Client and runtime keyword arguments
    project_id = (
        f"{callback_type}-callback-in-client"
        if in_client
        else f"{callback_type}-callback-in-runtime"
    )  # noqa: E501
    project_kwargs = {
        "id": project_id,
        "tasks_dir": tasks_dir,
    }
    runtime_kwargs = {
        "runtime_ctx": {
            "OUTPUT": wkdir / "output",
        },
        "rich_logging": False,
        "log_file": StringIO(),
    }
    if in_client:
        project_kwargs[f"on_{callback_type}"] = [callback_fn]
    else:
        runtime_kwargs[f"on_{callback_type}"] = [callback_fn]

    # Create project and run
    client = PrismProject(**project_kwargs)  # type: ignore
    try:
        client.run(**runtime_kwargs)  # type: ignore
    except Exception:
        pass

    # Callback output exists
    assert (wkdir / f"{callback_type}_callback.txt").is_file()
    callback_txt = _file_as_str(wkdir / f"{callback_type}_callback.txt")
    assert f"This was a {callback_type}!" == callback_txt
    os.unlink(wkdir / f"{callback_type}_callback.txt")

    # Check logs / other events
    assert any(
        [
            f"Running on_{callback_type} callbacks" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "Running callback_fn callback" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "FINISHED running callback_fn callback" in m
            for m in mock_console.return_value.messages
        ]
    )


def test_success_callbacks_in_client(mock_console):
    helper_for_testing_callbacks(
        mock_console,
        project_dir=Path(TEST_PROJECTS) / "005_simple_project_no_null_tasks",
        tasks_dir=Path(TEST_PROJECTS) / "005_simple_project_no_null_tasks" / "tasks",
        callback_type="success",
    )


def test_failure_callbacks_in_client(mock_console):
    helper_for_testing_callbacks(
        mock_console,
        project_dir=Path(TEST_PROJECTS) / "004_simple_project",
        tasks_dir=Path(TEST_PROJECTS) / "004_simple_project" / "modules",
        callback_type="failure",
    )


def test_success_callbacks_in_runtime(mock_console):
    helper_for_testing_callbacks(
        mock_console,
        project_dir=Path(TEST_PROJECTS) / "005_simple_project_no_null_tasks",
        tasks_dir=Path(TEST_PROJECTS) / "005_simple_project_no_null_tasks" / "tasks",
        callback_type="success",
        in_client=False,
    )


def test_failure_callbacks_in_runtime(mock_console):
    helper_for_testing_callbacks(
        mock_console,
        project_dir=Path(TEST_PROJECTS) / "004_simple_project",
        tasks_dir=Path(TEST_PROJECTS) / "004_simple_project" / "modules",
        callback_type="failure",
        in_client=False,
    )


def test_callbacks_with_import_path(mock_console):
    mock_console.return_value.messages = []

    wkdir = Path(TEST_PROJECTS) / "005_simple_project_no_null_tasks"
    os.chdir(wkdir)

    # Remove files in output folder
    _remove_files_in_output(wkdir)

    # Create project and run
    client = PrismProject(
        id="callback-using-import-path",
        tasks_dir=wkdir / "tasks",
        on_success=["additional_package.cli_callbacks.print_success"],
        package_lookups=[TEST_PROJECTS.parent],
    )
    client.run(
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        rich_logging=False,
        log_file=StringIO(),
    )

    # Check logs / other events
    assert any(
        [
            "Running on_success callbacks" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "Running print_success callback" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "FINISHED running print_success callback" in m
            for m in mock_console.return_value.messages
        ]
    )


def test_runtime_and_client_callback(mock_console):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = Path(TEST_PROJECTS) / "005_simple_project_no_null_tasks"
    os.chdir(wkdir)

    def callback_fn1():
        with open(wkdir / "on_success_callback1.txt", "w") as f:
            f.write("This was a on_success, output 1!")

    def callback_fn2():
        with open(wkdir / "on_success_callback2.txt", "w") as f:
            f.write("This was a on_success, output 2!")

    # Remove files in output folder
    _remove_files_in_output(wkdir)

    # Create project and run (without a run context)
    client = PrismProject(
        id="project-runtime-and-client-callbacks",
        tasks_dir=wkdir / "tasks",
        on_success=[callback_fn1],
    )
    client.run(
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        rich_logging=False,
        log_file=StringIO(),
        on_success=[callback_fn2],
    )

    # Console output
    assert any(
        [
            "Running on_success callbacks" in m
            for m in mock_console.return_value.messages
        ]
    )

    # Callback outputs and logs
    for output in [1, 2]:
        assert (wkdir / f"on_success_callback{output}.txt").is_file()
        callback_txt = _file_as_str(wkdir / f"on_success_callback{output}.txt")
        assert f"This was a on_success, output {output}!" == callback_txt
        os.unlink(wkdir / f"on_success_callback{output}.txt")

        # Check logs / other events
        assert any(
            [
                f"Running callback_fn{output} callback" in m
                for m in mock_console.return_value.messages
            ]
        )
        assert any(
            [
                f"FINISHED running callback_fn{output} callback" in m
                for m in mock_console.return_value.messages
            ]
        )


def test_connectors(mock_console):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = Path(TEST_PROJECTS) / "013_connectors"
    os.chdir(wkdir)

    # Remove files in output folder
    _remove_files_in_output(wkdir)

    # Define some connectors
    from prism.connectors import SnowflakeConnector

    snowflake_connector = SnowflakeConnector(
        id="snowflake-connector",
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )

    # Create project
    client = PrismProject(
        id="project-connectors",
        tasks_dir=wkdir / "tasks",
        connectors=[
            snowflake_connector,
        ],
    )

    # Run the ones without the bad adapter
    client.run(
        task_ids=[
            "snowflake_task.SnowflakeTask",
            "spark_task.PysparkTask",
        ],
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        rich_logging=False,
        log_file=StringIO(),
    )

    # Check outputs
    assert (wkdir / "output" / "machinery_sample.csv").is_file()
    assert (wkdir / "output" / "household_sample.csv").is_file()
    assert (wkdir / "output" / "machinery_sample_filtered.csv").is_file()
    assert (wkdir / "output" / "household_sample_filtered.csv").is_file()

    # Run with a bad adapter. This should raise an error.
    with pytest.raises(ValueError) as cm:
        client.run(
            task_ids=[
                "snowflake_task.SnowflakeTask",
                "bad_adapter.BadAdapterTask",
            ],
            runtime_ctx={
                "OUTPUT": wkdir / "output",
            },
            rich_logging=False,
            log_file=StringIO(),
        )
    expected_message_substr = "connector ID `snowflake_connector` not found run"
    assert expected_message_substr in str(cm.value)

    # Logs
    assert any(
        [
            "ERROR IN TASK bad_adapter.BadAdapterTask" in m
            for m in mock_console.return_value.messages
        ]
    )

    # Remove files in output folder
    _remove_files_in_output(wkdir)


def test_connectors_with_import_path(mock_console):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = Path(TEST_PROJECTS) / "013_connectors"
    os.chdir(wkdir)

    # Remove files in output folder
    _remove_files_in_output(wkdir)

    # Create project
    client = PrismProject(
        id="project-connectors",
        tasks_dir=wkdir / "tasks",
        connectors=[
            "additional_package.cli_connectors.snowflake_connector",
        ],
        package_lookups=[TEST_PROJECTS.parent],
    )

    # Run the ones without the bad adapter
    client.run(
        task_ids=[
            "snowflake_task.SnowflakeTask",
        ],
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        rich_logging=False,
        log_file=StringIO(),
    )

    # Check outputs
    assert (wkdir / "output" / "machinery_sample.csv").is_file()
    assert (wkdir / "output" / "household_sample.csv").is_file()

    # Remove files in output folder
    _remove_files_in_output(wkdir)


def test_package_lookups(mock_console):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = Path(TEST_PROJECTS) / "014_project_with_package_lookup"
    os.chdir(wkdir)

    # Remove files in output folder
    _remove_files_in_output(wkdir)

    # Create project and run (without a run context)
    client = PrismProject(
        id="project-package-lookups",
        tasks_dir=wkdir / "tasks",
        package_lookups=[TEST_CASE_WKDIR],
    )
    client.run(
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        rich_logging=False,
        log_file=StringIO(),
    )

    # Output file should have been created
    file_str_value = _file_as_str(wkdir / "output" / "task01.txt")
    expected_value = "Hello from module01.Task01"
    assert file_str_value == expected_value

    # Remove files in output folder
    _remove_files_in_output(wkdir)


def test_retries(mock_console):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = Path(TEST_PROJECTS) / "020_dec_retries"
    os.chdir(wkdir)

    # Remove files in output folder
    _remove_files_in_output(wkdir)

    # Create project and run (without a run context)
    client = PrismProject(
        id="project-retries",
        tasks_dir=wkdir / "tasks",
        package_lookups=[TEST_CASE_WKDIR],
    )
    with pytest.raises(NameError) as cm:
        client.run(
            runtime_ctx={
                "OUTPUT": wkdir / "output",
            },
            rich_logging=False,
            log_file=StringIO(),
        )
    expected_msg = "name 'hi' is not defined"
    assert expected_msg == str(cm.value)

    # Output
    assert any(
        ["RUNNING TASK load.load" in m for m in mock_console.return_value.messages]
    )
    assert any(
        ["ERROR IN TASK load.load" in m for m in mock_console.return_value.messages]
    )
    assert any(
        [
            "load.load failed...restarting immediately" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "RUNNING TASK load.load (RETRY 1)" in m
            for m in mock_console.return_value.messages
        ]
    )
    assert any(
        [
            "ERROR IN TASK load.load (RETRY 1)" in m
            for m in mock_console.return_value.messages
        ]
    )

    # Remove files in output folder
    _remove_files_in_output(wkdir)


def test_skip_task(mock_console):
    mock_console.return_value.messages = []

    # Set working directory
    wkdir = Path(TEST_PROJECTS) / "023_skipped_task"
    os.chdir(wkdir)

    # Create project and run (without a run context)
    client = PrismProject(
        id="project-skip", tasks_dir=wkdir / "tasks", package_lookups=[TEST_CASE_WKDIR]
    )
    client.run(
        runtime_ctx={
            "OUTPUT": wkdir / "output",
        },
        rich_logging=False,
        log_file=StringIO(),
    )

    # Output
    assert any(
        ["SKIPPING TASK task01.Task01" in m for m in mock_console.return_value.messages]
    )
