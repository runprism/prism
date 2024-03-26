# Standard library imports
from pathlib import Path
import pytest
from typing import Union


# Prism imports
import prism.exceptions
from prism.client.parser import ProjectParser
from prism.engine.compiler import _DagCompiler
from prism.tests.unit.test_compiled_projects import COMPILER_TEST_CASES


# Paths
TASK_REF_3NODES_DIR = Path(COMPILER_TEST_CASES) / "task_ref_3nodes"
TASK_REF_5NODES_DIR = Path(COMPILER_TEST_CASES) / "task_ref_5nodes"
TASK_REF_15NODES_DIR = Path(COMPILER_TEST_CASES) / "task_ref_15nodes"
TASK_REF_NOREFS_DIR = Path(COMPILER_TEST_CASES) / "task_ref_norefs"
TASK_REF_SELFREF_DIR = Path(COMPILER_TEST_CASES) / "task_ref_selfref"
DAG_CYCLE_DIR = Path(COMPILER_TEST_CASES) / "dag_cycle"


# Utils
def _parse_task_refs(
    project_dir: Path,
    tasks_dir: Union[str, Path],
    all_tasks_downstream: bool = True,
):
    parser = ProjectParser(
        project_dir,
        tasks_dir,
        all_tasks_downstream,
    )
    parsed_module_objs = parser.parse_all_modules()
    compiler = _DagCompiler(
        project_id="",
        run_slug=None,
        tasks_dir=tasks_dir,
        parsed_module_objs=parsed_module_objs,
        user_arg_task_ids=[],
        user_arg_all_downstream=all_tasks_downstream,
    )
    all_task_ids = compiler._get_all_task_ids(parsed_module_objs)
    _, refs, targets = compiler.parse_task_refs(
        all_task_ids,
        parsed_module_objs,
    )
    return {
        "task_ids": all_task_ids,
        "modules": parsed_module_objs,
        "compiler": compiler,
        "refs": refs,
        "targets": targets,
    }


def test_task_refs_3nodes():
    res = _parse_task_refs(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_3NODES_DIR,
        all_tasks_downstream=True,
    )
    task_ids = res["task_ids"]
    refs = res["refs"]
    assert len(task_ids) == 3
    assert len(refs["task02.Task02"]) == 1
    assert len(refs["task03.Task03"]) == 1
    assert len(refs["task01.Task01"]) == 0
    assert "task01.Task01" in refs["task02.Task02"]
    assert "task02.Task02" in refs["task03.Task03"]


def test_task_refs_5nodes():
    res = _parse_task_refs(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_5NODES_DIR,
        all_tasks_downstream=True,
    )
    task_ids = res["task_ids"]
    refs = res["refs"]
    assert len(task_ids) == 5
    expected_task_refs = {
        "taskA.Taska": [],
        "taskB.Taskb": ["taskA.Taska"],
        "taskC.Taskc": ["taskA.Taska"],
        "taskD.Taskd": ["taskB.Taskb", "taskA.Taska", "taskC.Taskc"],
        "taskE.Taske": ["taskA.Taska", "taskC.Taskc", "taskD.Taskd"],
    }
    for k, v in expected_task_refs.items():
        assert sorted(refs[k]) == sorted(v)


def test_task_refs_15nodes():
    res = _parse_task_refs(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_15NODES_DIR,
        all_tasks_downstream=True,
    )
    task_ids = res["task_ids"]
    refs = res["refs"]
    assert len(task_ids) == 16
    expected_task_refs = {
        "task01.Task01": [],
        "task02.Task02": ["task01.Task01"],
        "task03.Task03": ["task01.Task01"],
        "task04.Task04": ["task02.Task02", "task03.Task03"],
        "task05.Task05": ["task01.Task01"],
        "task06.Task06": ["task05.Task05"],
        "task07.Task07a": ["task04.Task04", "task06.Task06"],
        "task07.task_07b": ["task07.Task07a"],
        "task08.Task08": ["task01.Task01"],
        "task09.Task09": ["task05.Task05", "task08.Task08"],
        "task10.Task10": ["task01.Task01"],
        "task11.Task11": ["task07.Task07a", "task10.Task10"],
        "task12.Task12": ["task10.Task10"],
        "task13.Task13": ["task10.Task10"],
        "task14.Task14": ["task11.Task11"],
        "task15.Task15": ["task11.Task11"],
    }
    for k, v in expected_task_refs.items():
        assert sorted(refs[k]) == sorted(v)


def test_task_refs_norefs():
    res = _parse_task_refs(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_NOREFS_DIR,
        all_tasks_downstream=True,
    )
    task_ids = res["task_ids"]
    refs = res["refs"]
    assert len(task_ids) == 5
    for _, v in refs.items():
        assert len(v) == 0


def test_task_refs_cycle():
    res = _parse_task_refs(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=DAG_CYCLE_DIR,
        all_tasks_downstream=True,
    )
    task_ids = res["task_ids"]
    refs = res["refs"]
    assert len(task_ids) == 5
    expected_task_refs = {
        "moduleA.Taska": [],
        "moduleB.Taskb": ["moduleA.Taska", "moduleE.Taske"],
        "moduleC.Taskc": ["moduleA.Taska"],
        "moduleD.Taskd": ["moduleB.Taskb", "moduleA.Taska", "moduleC.Taskc"],
        "moduleE.Taske": ["moduleA.Taska", "moduleC.Taskc", "moduleD.Taskd"],
    }
    for k, v in expected_task_refs.items():
        assert sorted(refs[k]) == sorted(v)


#######################
# Create nodes, edges #
#######################


def test_create_nodes_edges_3nodes():
    res = _parse_task_refs(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_3NODES_DIR,
        all_tasks_downstream=True,
    )
    compiler: _DagCompiler = res["compiler"]
    nodes, edges = compiler.create_nodes_edges(res["refs"])

    # Nodes
    assert len(nodes) == 3
    assert sorted(["task01.Task01", "task02.Task02", "task03.Task03"]) == sorted(nodes)

    # Edges
    expected_edges = [
        ("task01.Task01", "task02.Task02"),
        ("task02.Task02", "task03.Task03"),
    ]
    assert sorted(edges) == sorted(expected_edges)


def test_create_nodes_edges_5nodes():
    res = _parse_task_refs(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_5NODES_DIR,
        all_tasks_downstream=True,
    )
    compiler: _DagCompiler = res["compiler"]
    nodes, edges = compiler.create_nodes_edges(res["refs"])

    # Nodes
    expected_nodes = [
        "taskA.Taska",
        "taskB.Taskb",
        "taskC.Taskc",
        "taskD.Taskd",
        "taskE.Taske",
    ]
    assert sorted(expected_nodes) == sorted(nodes)

    # Edges
    expected_edges = [
        ("taskA.Taska", "taskB.Taskb"),
        ("taskA.Taska", "taskC.Taskc"),
        ("taskB.Taskb", "taskD.Taskd"),
        ("taskA.Taska", "taskD.Taskd"),
        ("taskC.Taskc", "taskD.Taskd"),
        ("taskA.Taska", "taskE.Taske"),
        ("taskC.Taskc", "taskE.Taske"),
        ("taskD.Taskd", "taskE.Taske"),
    ]
    assert sorted(expected_edges) == sorted(edges)


def test_create_nodes_edges_15nodes():
    res = _parse_task_refs(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_15NODES_DIR,
        all_tasks_downstream=True,
    )
    compiler: _DagCompiler = res["compiler"]
    nodes, edges = compiler.create_nodes_edges(res["refs"])

    # Nodes
    expected_nodes = [
        "task01.Task01",
        "task02.Task02",
        "task03.Task03",
        "task04.Task04",
        "task05.Task05",
        "task06.Task06",
        "task07.Task07a",
        "task07.task_07b",
        "task08.Task08",
        "task09.Task09",
        "task10.Task10",
        "task11.Task11",
        "task12.Task12",
        "task13.Task13",
        "task14.Task14",
        "task15.Task15",
    ]
    assert sorted(expected_nodes) == sorted(nodes)

    # Edges
    expected_edges = [
        ("task01.Task01", "task02.Task02"),
        ("task01.Task01", "task03.Task03"),
        ("task02.Task02", "task04.Task04"),
        ("task03.Task03", "task04.Task04"),
        ("task01.Task01", "task05.Task05"),
        ("task05.Task05", "task06.Task06"),
        ("task04.Task04", "task07.Task07a"),
        ("task06.Task06", "task07.Task07a"),
        ("task07.Task07a", "task07.task_07b"),
        ("task01.Task01", "task08.Task08"),
        ("task05.Task05", "task09.Task09"),
        ("task08.Task08", "task09.Task09"),
        ("task01.Task01", "task10.Task10"),
        ("task07.Task07a", "task11.Task11"),
        ("task10.Task10", "task11.Task11"),
        ("task10.Task10", "task12.Task12"),
        ("task10.Task10", "task13.Task13"),
        ("task11.Task11", "task14.Task14"),
        ("task11.Task11", "task15.Task15"),
    ]
    assert sorted(expected_edges) == sorted(edges)


def test_create_nodes_edges_norefs():
    res = _parse_task_refs(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_NOREFS_DIR,
        all_tasks_downstream=True,
    )
    compiler: _DagCompiler = res["compiler"]
    nodes, edges = compiler.create_nodes_edges(res["refs"])

    # Nodes
    expected_nodes = [
        "moduleA.Taska",
        "moduleB.Taskb",
        "moduleC.Taskc",
        "moduleD.Taskd",
        "moduleE.Taske",
    ]
    assert sorted(expected_nodes) == sorted(nodes)

    # Edges
    assert edges == []


def test_create_nodes_edges_cycle():
    """
    Create nodes / edges for the DAG with a cycle
    """
    res = _parse_task_refs(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=DAG_CYCLE_DIR,
        all_tasks_downstream=True,
    )
    compiler: _DagCompiler = res["compiler"]
    nodes, edges = compiler.create_nodes_edges(res["refs"])

    # Nodes
    expected_nodes = [
        "moduleA.Taska",
        "moduleB.Taskb",
        "moduleE.Taske",
        "moduleC.Taskc",
        "moduleD.Taskd",
    ]
    assert sorted(expected_nodes) == sorted(nodes)

    # Edges
    expected_edges = [
        ("moduleA.Taska", "moduleB.Taskb"),
        ("moduleE.Taske", "moduleB.Taskb"),
        ("moduleA.Taska", "moduleC.Taskc"),
        ("moduleC.Taskc", "moduleD.Taskd"),
        ("moduleB.Taskb", "moduleD.Taskd"),
        ("moduleA.Taska", "moduleD.Taskd"),
        ("moduleD.Taskd", "moduleE.Taske"),
        ("moduleA.Taska", "moduleE.Taske"),
        ("moduleC.Taskc", "moduleE.Taske"),
    ]
    assert sorted(expected_edges) == sorted(edges)


##############
# Create DAG #
##############


def _create_dag(project_dir, tasks_dir):
    res = _parse_task_refs(
        project_dir=project_dir,
        tasks_dir=tasks_dir,
        all_tasks_downstream=True,
    )
    compiler: _DagCompiler = res["compiler"]
    nodes, edges = compiler.create_nodes_edges(res["refs"])
    return {
        "compiler": compiler,
        "dag": compiler.create_dag(nodes, edges),
    }


def test_create_dag_varying_complexity():
    """
    Create topsort for DAGs with different node counts and dependency complexities.
    These should not result in an error.
    """

    _create_dag(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_3NODES_DIR,
    )

    _create_dag(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_5NODES_DIR,
    )

    _create_dag(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_15NODES_DIR,
    )

    _create_dag(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_NOREFS_DIR,
    )


def test_create_dag_cycle():
    """
    Cycle in DAG throws an error
    """
    with pytest.raises(prism.exceptions.DAGException) as cm:
        _create_dag(
            project_dir=COMPILER_TEST_CASES,
            tasks_dir=DAG_CYCLE_DIR,
        )
    expected_msg = "invalid DAG, cycle found"
    cycled_tasks = ["moduleD.Taskd", "moduleB.Taskb", "moduleE.Taske"]
    assert expected_msg in str(cm.value)
    for _t in cycled_tasks:
        assert str(_t) in str(cm.value)


#########################
# Get node dependencies #
#########################


def test_node_dependencies_3nodes():
    """
    Get node dependencies from DAGs with 3 nodes.
    """
    res = _create_dag(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_3NODES_DIR,
    )
    compiler = res["compiler"]
    dag = res["dag"]
    node_deps_1 = compiler.get_node_dependencies(dag, ["task01.Task01"])
    node_deps_2 = compiler.get_node_dependencies(dag, ["task02.Task02"])
    node_deps_3 = compiler.get_node_dependencies(dag, ["task03.Task03"])
    node_deps_1_2 = compiler.get_node_dependencies(
        dag, ["task01.Task01", "task02.Task02"]
    )

    # Expected node dependencies
    expected_node_deps_1 = ["task01.Task01"]
    expected_node_deps_2 = ["task01.Task01", "task02.Task02"]
    expected_node_deps_3 = ["task01.Task01", "task02.Task02", "task03.Task03"]
    expected_node_deps_1_2 = ["task01.Task01", "task02.Task02"]

    # Asserts
    assert set(expected_node_deps_1) == set(node_deps_1)
    assert set(expected_node_deps_2) == set(node_deps_2)
    assert set(expected_node_deps_3) == set(node_deps_3)
    assert set(expected_node_deps_1_2) == set(node_deps_1_2)


def test_node_dependencies_5nodes():
    """
    Get node dependencies from DAGs with 5 nodes.
    """
    res = _create_dag(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_5NODES_DIR,
    )
    compiler = res["compiler"]
    dag = res["dag"]

    # Node dependencies
    node_deps_e = compiler.get_node_dependencies(dag, ["taskE.Taske"])
    expected_node_deps_e = [
        "taskA.Taska",
        "taskB.Taskb",
        "taskC.Taskc",
        "taskD.Taskd",
        "taskE.Taske",
    ]
    assert set(expected_node_deps_e) == set(node_deps_e)


def test_node_dependencies_15nodes():
    """
    Get node dependencies from DAGs with 15 nodes.
    """
    res = _create_dag(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_15NODES_DIR,
    )
    compiler = res["compiler"]
    dag = res["dag"]

    # Node dependencies
    node_deps_7b = compiler.get_node_dependencies(dag, ["task07.task_07b"])
    expected_node_deps_7 = [
        "task07.task_07b",
        "task07.Task07a",
        "task04.Task04",
        "task06.Task06",
        "task02.Task02",
        "task03.Task03",
        "task05.Task05",
        "task01.Task01",
    ]
    assert set(expected_node_deps_7) == set(node_deps_7b)

    # Set of nodes with all nodes as their dependencies
    node_deps_all = compiler.get_node_dependencies(
        dag,
        [
            "task07.task_07b",
            "task14.Task14",
            "task15.Task15",
            "task12.Task12",
            "task13.Task13",
            "task09.Task09",
        ],
    )
    expected_node_deps_all = [
        "task01.Task01",
        "task02.Task02",
        "task03.Task03",
        "task04.Task04",
        "task05.Task05",
        "task06.Task06",
        "task07.Task07a",
        "task07.task_07b",
        "task08.Task08",
        "task09.Task09",
        "task10.Task10",
        "task11.Task11",
        "task12.Task12",
        "task13.Task13",
        "task14.Task14",
        "task15.Task15",
    ]
    assert set(expected_node_deps_all) == set(node_deps_all)


##################
# Create topsort #
##################


def _parent_before_child(parent_list, parent: str, child: Union[str, list]):
    """
    For our simple, 3-node DAG, we know what the topsort list should look like.
    For the others, there are multiple possible orderings. We initially tried to
    sort the result of the NetworkX `all_topological_sorts` function returns a
    generator, but this turned out to be too computationally expensive when the
    number of possible orderings was high. Therefore, rather than test the value
    of the list itself, we will confirm that parent nodes appear before their
    children in the list.
    """
    idx1 = parent_list.index(parent)
    if isinstance(child, str):
        idx2 = parent_list.index(child)
        assert idx1 < idx2
    elif isinstance(child, list):
        for node in child:
            idx2 = parent_list.index(node)
            assert idx1 < idx2


def test_topsort_3nodes():
    res = _parse_task_refs(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_3NODES_DIR,
        all_tasks_downstream=True,
    )
    task_ids = res["task_ids"]
    modules = res["modules"]
    compiler: _DagCompiler = res["compiler"]
    _, topsort = compiler.create_topsort(
        all_task_ids=task_ids,
        user_arg_task_ids=task_ids,
        parsed_module_objs=modules,
    )
    expected_topsort = ["task01.Task01", "task02.Task02", "task03.Task03"]
    assert expected_topsort == topsort


def test_topsort_5nodes():
    res = _parse_task_refs(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_5NODES_DIR,
        all_tasks_downstream=True,
    )
    task_ids = res["task_ids"]
    modules = res["modules"]
    compiler: _DagCompiler = res["compiler"]
    _, topsort = compiler.create_topsort(
        all_task_ids=task_ids,
        user_arg_task_ids=task_ids,
        parsed_module_objs=modules,
    )

    # Topsort ordering
    _parent_before_child(
        topsort,
        "taskA.Taska",
        ["taskB.Taskb", "taskC.Taskc", "taskD.Taskd", "taskE.Taske"],
    )
    _parent_before_child(
        topsort,
        "taskB.Taskb",
        "taskD.Taskd",
    )
    _parent_before_child(
        topsort,
        "taskC.Taskc",
        ["taskD.Taskd", "taskE.Taske"],
    )
    _parent_before_child(topsort, "taskD.Taskd", "taskE.Taske")


def test_topsort_15nodes():
    """
    Create the topological sort for the DAG with 15 nodes
    """
    res = _parse_task_refs(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_15NODES_DIR,
        all_tasks_downstream=True,
    )
    task_ids = res["task_ids"]
    modules = res["modules"]
    compiler: _DagCompiler = res["compiler"]
    _, topsort = compiler.create_topsort(
        all_task_ids=task_ids,
        user_arg_task_ids=task_ids,
        parsed_module_objs=modules,
    )

    # Topsort ordering
    _parent_before_child(
        topsort,
        "task01.Task01",
        [
            "task10.Task10",
            "task02.Task02",
            "task03.Task03",
            "task05.Task05",
            "task08.Task08",
        ],
    )
    _parent_before_child(
        topsort,
        "task10.Task10",
        ["task11.Task11", "task12.Task12", "task13.Task13"],
    )
    _parent_before_child(topsort, "task02.Task02", "task04.Task04")
    _parent_before_child(topsort, "task03.Task03", "task04.Task04")
    _parent_before_child(topsort, "task05.Task05", ["task06.Task06", "task09.Task09"])
    _parent_before_child(topsort, "task08.Task08", "task09.Task09")
    _parent_before_child(topsort, "task11.Task11", ["task14.Task14", "task15.Task15"])
    _parent_before_child(topsort, "task04.Task04", "task07.Task07a")
    _parent_before_child(topsort, "task06.Task06", "task07.Task07a")
    _parent_before_child(topsort, "task07.Task07a", "task07.task_07b")


def test_topsort_no_refs():
    """
    DAGs with no dependencies should include all tasks (assuming a subset of
    tasks is not explicitly specified).
    """
    res = _parse_task_refs(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_NOREFS_DIR,
        all_tasks_downstream=True,
    )
    task_ids = res["task_ids"]
    modules = res["modules"]
    compiler: _DagCompiler = res["compiler"]
    _, topsort = compiler.create_topsort(
        all_task_ids=task_ids,
        user_arg_task_ids=task_ids,
        parsed_module_objs=modules,
    )

    # All tasks must be in DAG
    assert len(task_ids) == len(topsort)


def test_topsort_subset():
    res = _parse_task_refs(
        project_dir=COMPILER_TEST_CASES,
        tasks_dir=TASK_REF_15NODES_DIR,
        all_tasks_downstream=False,
    )
    task_ids = res["task_ids"]
    modules = res["modules"]
    compiler: _DagCompiler = res["compiler"]
    _, topsort_15_7 = compiler.create_topsort(
        all_task_ids=task_ids,
        user_arg_task_ids=["task07.Task07a"],
        parsed_module_objs=modules,
    )
    _, topsort_15_all = compiler.create_topsort(
        all_task_ids=task_ids,
        user_arg_task_ids=[
            "task07.task_07b",
            "task14.Task14",
            "task15.Task15",
            "task12.Task12",
            "task13.Task13",
            "task09.Task09",
        ],
        parsed_module_objs=modules,
    )
    actual_topsort_15nodes_7 = [
        "task01.Task01",
        "task02.Task02",
        "task03.Task03",
        "task04.Task04",
        "task05.Task05",
        "task06.Task06",
        "task07.Task07a",
    ]
    assert set(actual_topsort_15nodes_7) == set(topsort_15_7)
    assert set(task_ids) == set(topsort_15_all)
