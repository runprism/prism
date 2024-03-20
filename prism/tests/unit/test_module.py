# Standard library imports
import ast
from pathlib import Path

import pytest

# Prism imports
import prism.exceptions
from prism.engine.module import _get_func_args, _PrismModule

# Paths
UNIT_TEST_WKDIR = Path(__file__).parent
TASK_TEST_CASES = UNIT_TEST_WKDIR / "test_tasks"


# Tasks that are classes
ONE_PRISM_TASK = Path("cls_one_prism_task.py")
NO_PRISM_TASK = Path("cls_no_prism_task.py")
MULTIPLE_PRISM_TASKS = Path("cls_multiple_prism_tasks.py")
DIFF_IMPORT_STRUCTURE = Path("cls_diff_import_structure.py")
OTHER_CLASSES = Path("cls_other_classes.py")
TASKS_REFS = Path("cls_tasks_refs.py")
TASK_WITH_TARGET = Path("cls_task_with_target.py")
BAD_RUN_EXTRA_ARG = Path("cls_bad_run_extra_arg.py")
NO_RUN_FUNC = Path("cls_no_run_func.py")
DIFF_TASK_ID = Path("cls_task_with_id.py")

# Tasks that are decorated functions
DEC_ONE_PRISM_TASK = Path("dec_one_prism_task.py")
DEC_NO_PRISM_TASK = Path("dec_no_prism_task.py")
DEC_MULTIPLE_PRISM_TASKS = Path("dec_multiple_prism_tasks.py")
DEC_DIFF_DECORATOR_STRUCTURE = Path("dec_diff_decorator_structure.py")
DEC_OTHER_FUNCTIONS = Path("dec_other_functions.py")
DEC_TASKS_REFS = Path("dec_tasks_refs.py")
DEC_TASK_WITH_TARGET = Path("dec_task_with_target.py")
DEC_BAD_RUN_EXTRA_ARG = Path("dec_bad_run_extra_arg.py")
DEC_BAD_DEC_NO_PARENTHESES = Path("dec_bad_dec_no_parentheses.py")
DEC_DIFF_TASK_ID = Path("dec_task_with_id.py")


# Tests
def test_one_prism_task():
    # Task is a class that inherits PrismTask class
    cls_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(ONE_PRISM_TASK),
    )
    cls_prism_tasks = cls_module.prism_task_nodes
    assert len(cls_prism_tasks.keys()) == 1
    cls_expected_key = ONE_PRISM_TASK.name.replace(".py", "") + ".NormalPrismTask"
    assert cls_expected_key in cls_prism_tasks
    assert isinstance(cls_prism_tasks[cls_expected_key], ast.ClassDef)

    # Task is a function that is a decorated function
    dec_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(DEC_ONE_PRISM_TASK),
    )
    dec_prism_tasks = dec_module.prism_task_nodes
    assert len(dec_prism_tasks.keys()) == 1
    dec_expected_key = DEC_ONE_PRISM_TASK.name.replace(".py", "") + ".task_function"
    assert dec_expected_key in dec_prism_tasks
    assert isinstance(dec_prism_tasks[dec_expected_key], ast.FunctionDef)


def test_no_prism_task():
    # Task is a class that inherits PrismTask class
    cls_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(NO_PRISM_TASK),
    )
    cls_prism_tasks = cls_module.prism_task_nodes
    assert len(cls_prism_tasks.keys()) == 0

    with pytest.raises(ValueError) as cm:
        cls_module.parse("example_task_id")
    expected_msg = f"Error in `{str(NO_PRISM_TASK)}`: could not find task with ID `example_task_id`!"  # noqa: E501
    assert str(cm.value) == expected_msg

    # Task is a function that is a decorated function
    dec_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(DEC_NO_PRISM_TASK),
    )
    dec_prism_tasks = dec_module.prism_task_nodes
    assert len(dec_prism_tasks.keys()) == 0


def test_multiple_prism_task():
    # Task is a class that inherits PrismTask class
    cls_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(MULTIPLE_PRISM_TASKS),
    )
    cls_prism_tasks = cls_module.prism_task_nodes
    assert len(cls_prism_tasks.keys()) == 2
    assert (
        MULTIPLE_PRISM_TASKS.name.replace(".py", "") + ".FirstPrismTask"
        in cls_prism_tasks
    )  # noqa: E501
    assert (
        MULTIPLE_PRISM_TASKS.name.replace(".py", "") + ".SecondPrismTask"
        in cls_prism_tasks
    )  # noqa: E501

    # Parse
    cls_refs_and_targets = cls_module.parse(
        MULTIPLE_PRISM_TASKS.name.replace(".py", "") + ".SecondPrismTask"
    )
    assert len(cls_refs_and_targets.refs) == 0
    assert len(cls_refs_and_targets.targets) == 0

    # Task is a function that is a decorated function
    dec_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(DEC_MULTIPLE_PRISM_TASKS),
    )
    dec_prism_tasks = dec_module.prism_task_nodes
    assert len(dec_prism_tasks.keys()) == 2
    assert (
        DEC_MULTIPLE_PRISM_TASKS.name.replace(".py", "") + ".task_function_1"
        in dec_prism_tasks
    )  # noqa: E501
    assert (
        DEC_MULTIPLE_PRISM_TASKS.name.replace(".py", "") + ".task_function_2"
        in dec_prism_tasks
    )  # noqa: E501

    # Parse
    dec_refs_and_targets = dec_module.parse(
        DEC_MULTIPLE_PRISM_TASKS.name.replace(".py", "") + ".task_function_2"
    )
    assert len(dec_refs_and_targets.refs) == 0
    assert len(dec_refs_and_targets.targets) == 0


def test_diff_import_structure():
    # Task is a class that inherits PrismTask class
    cls_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(DIFF_IMPORT_STRUCTURE),
    )
    cls_prism_tasks = cls_module.prism_task_nodes
    assert len(cls_prism_tasks.keys()) == 1
    assert (
        DIFF_IMPORT_STRUCTURE.name.replace(".py", "") + ".DiffImportStructure"
        in cls_prism_tasks
    )  # noqa: E501

    # Task is a function that is a decorated function
    dec_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(DEC_DIFF_DECORATOR_STRUCTURE),
    )
    dec_prism_tasks = dec_module.prism_task_nodes
    assert len(dec_prism_tasks.keys()) == 1
    assert (
        DEC_DIFF_DECORATOR_STRUCTURE.name.replace(".py", "")
        + ".task_fn_different_decorator_structure"
        in dec_prism_tasks
    )  # noqa: E501


def test_other_classes_functions():
    # Task is a class that inherits PrismTask class
    cls_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(OTHER_CLASSES),
    )
    cls_prism_tasks = cls_module.prism_task_nodes
    assert len(cls_prism_tasks.keys()) == 1
    assert OTHER_CLASSES.name.replace(".py", "") + ".OnlyPrismTask" in cls_prism_tasks  # noqa: E501

    # Task is a function that is a decorated function
    dec_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(DEC_OTHER_FUNCTIONS),
    )
    dec_prism_tasks = dec_module.prism_task_nodes
    assert len(dec_prism_tasks.keys()) == 1
    assert (
        DEC_OTHER_FUNCTIONS.name.replace(".py", "") + ".task_function"
        in dec_prism_tasks
    )  # noqa: E501


def test_tasks_with_custom_ids():
    # Task is a class that inherits PrismTask class
    cls_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(DIFF_TASK_ID),
    )
    cls_prism_tasks = cls_module.prism_task_nodes
    assert len(cls_prism_tasks.keys()) == 1
    assert "cls_custom_task_id" in cls_prism_tasks

    # Task is a function that is a decorated function
    dec_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(DEC_DIFF_TASK_ID),
    )
    dec_prism_tasks = dec_module.prism_task_nodes
    assert len(dec_prism_tasks.keys()) == 1
    assert "dec_custom_task_id" in dec_prism_tasks


def test_task_with_target():
    # Task is a class that inherits PrismTask class
    cls_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(TASK_WITH_TARGET),
    )
    cls_prism_tasks = cls_module.prism_task_nodes
    assert len(cls_prism_tasks.keys()) == 1
    cls_expected_key = TASK_WITH_TARGET.name.replace(".py", "") + ".TaskWithTarget"
    assert cls_expected_key in cls_prism_tasks

    # Run function arguments
    run_func = cls_module.get_run_func(cls_prism_tasks[cls_expected_key])
    run_func_args = _get_func_args(run_func)
    assert ["self"] == run_func_args

    # Parse
    cls_refs_and_targets = cls_module.parse(cls_expected_key)
    assert len(cls_refs_and_targets.refs) == 0
    assert len(cls_refs_and_targets.targets) == 1
    assert "os.path.join(os.getcwd(), 'temp')" == cls_refs_and_targets.targets[0]

    # Task is a function that is a decorated function
    dec_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(DEC_TASK_WITH_TARGET),
    )
    dec_prism_tasks = dec_module.prism_task_nodes
    assert len(dec_prism_tasks.keys()) == 1
    dec_expected_key = (
        DEC_TASK_WITH_TARGET.name.replace(".py", "") + ".task_with_target"
    )  # noqa: E501
    assert dec_expected_key in dec_prism_tasks

    # Run function arguments
    run_func = dec_module.get_run_func(dec_prism_tasks[dec_expected_key])
    run_func_args = _get_func_args(run_func)
    assert len(run_func_args) == 0

    # Parse
    dec_refs_and_targets = dec_module.parse(dec_expected_key)
    assert len(dec_refs_and_targets.refs) == 2
    sources = [r.source for r in dec_refs_and_targets.refs]
    assert "hello.py" in sources
    assert "world.py" in sources
    assert len(dec_refs_and_targets.targets) == 1
    assert "Path(__file__) / 'test.txt'" == dec_refs_and_targets.targets[0]


def test_tasks_refs():
    # Task is a class that inherits PrismTask class
    cls_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(TASKS_REFS),
    )
    cls_prism_tasks = cls_module.prism_task_nodes
    assert len(cls_prism_tasks.keys()) == 1
    cls_expected_key = TASKS_REFS.name.replace(".py", "") + ".TasksRefs"
    assert cls_expected_key in cls_prism_tasks

    # Run function arguments
    run_func = cls_module.get_run_func(cls_prism_tasks[cls_expected_key])
    run_func_args = _get_func_args(run_func)
    assert ["self"] == run_func_args

    # Parse
    cls_refs_and_targets = cls_module.parse(cls_expected_key)
    assert len(cls_refs_and_targets.refs) == 4
    assert len(cls_refs_and_targets.targets) == 0
    cls_sources = [r.source for r in cls_refs_and_targets.refs]
    assert "func_0" in cls_sources
    assert "func_1" in cls_sources
    assert "hello" in cls_sources
    assert "world" in cls_sources

    # Task is a function that is a decorated function
    dec_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(DEC_TASKS_REFS),
    )
    dec_prism_tasks = dec_module.prism_task_nodes
    assert len(dec_prism_tasks.keys()) == 1
    dec_expected_key = DEC_TASKS_REFS.name.replace(".py", "") + ".task_with_refs"
    assert dec_expected_key in dec_prism_tasks

    # Run function arguments
    run_func = dec_module.get_run_func(dec_prism_tasks[dec_expected_key])
    run_func_args = _get_func_args(run_func)
    assert len(run_func_args) == 0

    # Parse
    dec_refs_and_targets = dec_module.parse(dec_expected_key)
    assert len(dec_refs_and_targets.refs) == 4
    assert len(dec_refs_and_targets.targets) == 1
    assert dec_refs_and_targets.targets[0] == "Path(__file__) / 'test.txt'"
    dec_sources = [r.source for r in dec_refs_and_targets.refs]
    assert "func_0" in dec_sources
    assert "func_1" in dec_sources
    assert "hello" in dec_sources
    assert "world" in dec_sources


def test_bad_run_arguments():
    def _get_args(task):
        module = _PrismModule(
            project_dir=UNIT_TEST_WKDIR,
            root_task_dir=TASK_TEST_CASES,
            module_task_relpath=str(task),
        )
        prism_task_nodes = module.prism_task_nodes
        assert len(prism_task_nodes.keys()) == 1

        # Get Prism task
        prism_task = list(prism_task_nodes.values())[0]
        run_func = module.get_run_func(prism_task)
        run_func_args = _get_func_args(run_func)
        return run_func_args

    # Extra arg
    cls_run_func_args = _get_args(BAD_RUN_EXTRA_ARG)
    assert len(cls_run_func_args) == 2
    assert "self" in cls_run_func_args
    assert "extra_arg" in cls_run_func_args

    # Missing arg
    dec_run_func_args = _get_args(DEC_BAD_RUN_EXTRA_ARG)
    assert len(dec_run_func_args) == 1
    assert "extra_arg" in dec_run_func_args


def test_no_run_function_in_cls():
    # Task is a class that inherits PrismTask class
    cls_module = _PrismModule(
        project_dir=UNIT_TEST_WKDIR,
        root_task_dir=TASK_TEST_CASES,
        module_task_relpath=str(NO_RUN_FUNC),
    )
    cls_prism_tasks = cls_module.prism_task_nodes
    assert len(cls_prism_tasks.keys()) == 1
    cls_expected_key = NO_RUN_FUNC.name.replace(".py", "") + ".NoRunFunc"
    assert cls_expected_key in cls_prism_tasks

    # Parsing should raise an error
    with pytest.raises(prism.exceptions.ParserException) as cm:
        cls_module.parse(cls_expected_key)
    cls_expected_msg = (
        f"Error in task `{cls_expected_key}` in `{NO_RUN_FUNC}`: no run function!"  # noqa: E501
    )
    assert str(cm.value) == cls_expected_msg


def test_no_parentheses_in_decorator():
    # Task is a function that is a decorated function
    with pytest.raises(prism.exceptions.RuntimeException) as cm:
        _PrismModule(
            project_dir=UNIT_TEST_WKDIR,
            root_task_dir=TASK_TEST_CASES,
            module_task_relpath=str(DEC_BAD_DEC_NO_PARENTHESES),
        )
    expected_msg = "`task` decorator not properly specified...try adding parentheses to it, e.g., `@task()`"  # noqa: E501
    assert str(cm.value) == expected_msg
