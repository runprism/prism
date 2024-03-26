# Standard library imports
import networkx
from pathlib import Path
import pytest


# Prism imports
import prism.exceptions
from prism.client.parser import ProjectParser
from prism.engine.module import _PrismModule


# Paths
UNIT_TEST_WKDIR = Path(__file__).parent
TASK_TEST_CASES = UNIT_TEST_WKDIR / "test_tasks"
EXAMPLE_TASKS_DIR = TASK_TEST_CASES / "example_tasks_dir"


def test_parser_no_task_dir():
    with pytest.raises(prism.exceptions.CompileException) as cm:
        ProjectParser(
            project_dir=TASK_TEST_CASES,
            tasks_dir=TASK_TEST_CASES / "dummy_dir",
            all_tasks_downstream=True,
        )
    expected_msg = f'`{TASK_TEST_CASES / "dummy_dir"}` is not a directory!'
    assert str(cm.value) == expected_msg


def test_parser():
    parser = ProjectParser(
        project_dir=TASK_TEST_CASES,
        tasks_dir=EXAMPLE_TASKS_DIR,
        all_tasks_downstream=True,
    )
    all_modules = parser._get_all_modules_in_dir(EXAMPLE_TASKS_DIR)
    assert len(all_modules) == 5
    assert "func_0.py" in all_modules
    assert "func_1.py" in all_modules
    assert "hello.py" in all_modules
    assert "world.py" in all_modules
    assert "nested/foo.py" in all_modules

    # Module objects
    module_objs = parser.parse_all_modules()
    for x in module_objs:
        assert isinstance(x, _PrismModule)

    # DAG should compile with all tasks
    parser.compile_dag(
        project_id="",
        run_slug=None,
        tasks_dir=EXAMPLE_TASKS_DIR,
        parsed_module_objs=module_objs,
        user_arg_task_ids=[],
        user_arg_all_downstream=True,
    )

    # DAG should compile with a subset of tasks
    parser.compile_dag(
        project_id="",
        run_slug=None,
        tasks_dir=EXAMPLE_TASKS_DIR,
        parsed_module_objs=module_objs,
        user_arg_task_ids=["hello", "world"],
        user_arg_all_downstream=True,
    )

    # If we pass in tasks that do not exist, the DAG should not compile
    with pytest.raises(networkx.exception.NetworkXError) as cm:
        parser.compile_dag(
            project_id="",
            run_slug=None,
            tasks_dir=EXAMPLE_TASKS_DIR,
            parsed_module_objs=module_objs,
            user_arg_task_ids=["foo"],
            user_arg_all_downstream=True,
        )
    expected_msg = "The node foo is not in the digraph."
    assert str(cm.value) == expected_msg
