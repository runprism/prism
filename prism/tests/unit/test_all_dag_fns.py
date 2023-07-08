"""
Unit testing for functions used to parse tasks.ref(...) references within a DAG
"""


###########
# Imports #
###########

# Standard library imports
import argparse
import unittest
from pathlib import Path
from typing import Any, List, Union, Optional

# Prism imports
import prism.exceptions
from prism.mixins.compile import CompileMixin
from prism.infra import compiler
import prism.prism_logging
from prism.tests.unit.test_all_things_dag import DAG_TEST_CASES
from prism.tests.unit.test_all_things_dag.task_ref_3nodes import (
    TASK_REF_3NODES_MODULE_LIST,
    TASK_REF_3NODES_TASK_LIST,
)
from prism.tests.unit.test_all_things_dag.task_ref_5nodes import (
    TASK_REF_5NODES_MODULE_LIST,
    TASK_REF_5NODES_TASK_LIST,
)
from prism.tests.unit.test_all_things_dag.task_ref_15nodes import (
    TASK_REF_15NODES_MODULE_LIST,
    TASK_REF_15NODES_TASK_LIST,
)
from prism.tests.unit.test_all_things_dag.task_ref_norefs import (
    TASK_REF_NOREFS_MODULE_LIST,
    TASK_REF_NOREFS_TASK_LIST,
)
from prism.tests.unit.test_all_things_dag.task_ref_selfref import (
    TASK_REF_SELFREF_MODULE_LIST,
    TASK_REF_SELFREF_TASK_LIST,
)
from prism.tests.unit.test_all_things_dag.dag_cycle import (
    DAG_CYCLE_MODULE_LIST,
    DAG_CYCLE_TASK_LIST,
)

# Set up logger
args = argparse.Namespace()
args.log_level = "info"
prism.prism_logging.set_up_logger(args)


###########################
# Paths / class instances #
###########################

# Task directories
TASK_REF_3NODES_DIR = Path(DAG_TEST_CASES) / 'task_ref_3nodes'
TASK_REF_5NODES_DIR = Path(DAG_TEST_CASES) / 'task_ref_5nodes'
TASK_REF_15NODES_DIR = Path(DAG_TEST_CASES) / 'task_ref_15nodes'
TASK_REF_NOREFS_DIR = Path(DAG_TEST_CASES) / 'task_ref_norefs'
TASK_REF_SELFREF_DIR = Path(DAG_TEST_CASES) / 'task_ref_selfref'
DAG_CYCLE_DIR = Path(DAG_TEST_CASES) / 'dag_cycle'


##############################
# Test case class definition #
##############################

class TestAllDagFunctions(
    unittest.TestCase,
    CompileMixin
):

    def _convert_task_refs_to_str(self, task_refs: dict):
        task_refs_str = {}
        for k, v in task_refs.items():
            new_k = str(k)
            if isinstance(v, Path):
                new_v = str(v)
            elif isinstance(v, list):
                new_v = [str(p) for p in v]
            elif v is None:
                new_v = None
            else:
                raise Exception
            task_refs_str[new_k] = new_v
        return task_refs_str

    def _convert_nodes_to_str(self, nodes: list):
        return [str(p) for p in nodes]

    def _convert_edges_to_str(self, edges: list):
        edges_str = []
        for e in edges:
            new_edge = (str(e[0]), str(e[1]))
            edges_str.append(new_edge)
        return edges_str

    def _convert_topsort_to_str(self, topsort: list):
        return [str(p) for p in topsort]

    ###################
    # Task references #
    ###################

    def _parse_task_refs(self,
        project_dir: Path,
        module_list: List[Any],
        tasks_dir: Path,
        task_list: List[str]
    ):
        all_parsed_tasks = self.parse_all_tasks(
            module_list,
            tasks_dir=tasks_dir
        )
        all_task_names = self.get_task_names(all_parsed_tasks)
        self.assertEqual(sorted(task_list), sorted(all_task_names))

        # Task references
        dag_compiler = compiler.DagCompiler(
            project_dir=project_dir,
            tasks_dir=tasks_dir,
            compiled_dir=None,
            all_tasks=all_task_names,
            parsed_tasks=all_parsed_tasks,
            user_arg_tasks=[],
            user_arg_all_downstream=False,
            project=None,
        )
        task_refs = dag_compiler.parse_task_refs(
            tasks=all_task_names,
            parsed_tasks=all_parsed_tasks
        )

        # Return
        return task_refs, dag_compiler

    def test_task_refs_3nodes(self):
        """
        Parse task references for DAG with 3 nodes
        """
        task_refs_3nodes, _ = self._parse_task_refs(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_3NODES_MODULE_LIST,
            tasks_dir=TASK_REF_3NODES_DIR,
            task_list=TASK_REF_3NODES_TASK_LIST,
        )
        expected_task_refs = {
            'task01.Task01': [],
            'task02.Task02': ['task01.Task01'],
            'task03.Task03': ['task02.Task02']
        }
        self.assertEqual(expected_task_refs, task_refs_3nodes)

    def test_task_refs_5nodes(self):
        """
        Parse task references for DAG with 5 nodes
        """
        task_refs_5nodes, _ = self._parse_task_refs(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_5NODES_MODULE_LIST,
            tasks_dir=TASK_REF_5NODES_DIR,
            task_list=TASK_REF_5NODES_TASK_LIST,
        )
        expected_task_refs = {
            'taskA.Taska': [],
            'taskB.Taskb': ['taskA.Taska'],
            'taskC.Taskc': ['taskA.Taska'],
            'taskD.Taskd': ['taskB.Taskb', 'taskA.Taska', 'taskC.Taskc'],
            'taskE.Taske': ['taskA.Taska', 'taskC.Taskc', 'taskD.Taskd'],
        }
        for k, _ in expected_task_refs.items():
            self.assertEqual(sorted(expected_task_refs[k]), sorted(task_refs_5nodes[k]))

    def test_task_refs_15nodes(self):
        """
        Parse task references for DAG with 15 nodes. This DAG also has some local /
        function-based tasks.
        """
        task_refs_15nodes, _ = self._parse_task_refs(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_15NODES_MODULE_LIST,
            tasks_dir=TASK_REF_15NODES_DIR,
            task_list=TASK_REF_15NODES_TASK_LIST,
        )
        expected_task_refs = {
            'task01.Task01': [],
            'task02.Task02': ['task01.Task01'],
            'task03.Task03': ['task01.Task01'],
            'task04.Task04': ['task02.Task02', 'task03.Task03'],
            'task05.Task05': ['task01.Task01'],
            'task06.Task06': ['task05.Task05'],
            'task07.Task07a': ['task04.Task04', 'task06.Task06'],
            'task07.task_07b': ['task07.Task07a'],
            'task08.Task08': ['task01.Task01'],
            'task09.Task09': ['task05.Task05', 'task08.Task08'],
            'task10.Task10': ['task01.Task01'],
            'task11.Task11': ['task07.Task07a', 'task10.Task10'],
            'task12.Task12': ['task10.Task10'],
            'task13.Task13': ['task10.Task10'],
            'task14.Task14': ['task11.Task11'],
            'task15.Task15': ['task11.Task11'],
        }
        for k, _ in expected_task_refs.items():
            self.assertEqual(
                sorted(expected_task_refs[k]),
                sorted(task_refs_15nodes[k])
            )

    def test_task_refs_norefs(self):
        """
        Parse task references for DAG with 5 nodes but no task refs
        """
        task_refs_norefs, _ = self._parse_task_refs(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_NOREFS_MODULE_LIST,
            tasks_dir=TASK_REF_NOREFS_DIR,
            task_list=TASK_REF_NOREFS_TASK_LIST,
        )
        expected_task_refs = {
            'moduleA.Taska': [],
            'moduleB.Taskb': [],
            'moduleC.Taskc': [],
            'moduleD.Taskd': [],
            'moduleE.Taske': [],
        }
        for k, _ in expected_task_refs.items():
            self.assertEqual(sorted(expected_task_refs[k]), sorted(task_refs_norefs[k]))

    def test_task_refs_selfref(self):
        """
        Parse task references for DAG with 5 nodes but no task refs
        """
        with self.assertRaises(prism.exceptions.ReferenceException) as cm:
            _, _ = self._parse_task_refs(
                project_dir=DAG_TEST_CASES,
                module_list=TASK_REF_SELFREF_MODULE_LIST,
                tasks_dir=TASK_REF_SELFREF_DIR,
                task_list=TASK_REF_SELFREF_TASK_LIST,
            )
        expected_msg = 'Are you trying to access a task in the same module? If so, use only the task name as your tasks.ref() argument and set `local = True`'  # noqa
        self.assertEqual(expected_msg, str(cm.exception))

    def test_task_refs_cycle(self):
        """
        Parse task references for DAG with 5 nodes but no task refs
        """
        task_refs_norefs, _ = self._parse_task_refs(
            project_dir=DAG_TEST_CASES,
            module_list=DAG_CYCLE_MODULE_LIST,
            tasks_dir=DAG_CYCLE_DIR,
            task_list=DAG_CYCLE_TASK_LIST,
        )
        expected_task_refs = {
            'moduleA.Taska': [],
            'moduleB.Taskb': ['moduleA.Taska', 'moduleE.Taske'],
            'moduleC.Taskc': ['moduleA.Taska'],
            'moduleD.Taskd': ['moduleB.Taskb', 'moduleA.Taska', 'moduleC.Taskc'],
            'moduleE.Taske': ['moduleA.Taska', 'moduleC.Taskc', 'moduleD.Taskd'],
        }
        for k, _ in expected_task_refs.items():
            self.assertEqual(sorted(expected_task_refs[k]), sorted(task_refs_norefs[k]))

    #######################
    # Create nodes, edges #
    #######################

    def _create_nodes_edges(self,
        project_dir: Path,
        module_list: List[Any],
        tasks_dir: Path,
        task_list: List[str]
    ):
        task_refs, dag_compiler = self._parse_task_refs(
            project_dir=project_dir,
            module_list=module_list,
            tasks_dir=tasks_dir,
            task_list=task_list,
        )
        nodes, edges = dag_compiler.create_nodes_edges(task_refs)
        return nodes, edges, dag_compiler

    def test_create_nodes_edges_3nodes(self):
        """
        Create nodes / edges for the DAG with 3 nodes.
        """
        nodes, edges, _ = self._create_nodes_edges(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_3NODES_MODULE_LIST,
            tasks_dir=TASK_REF_3NODES_DIR,
            task_list=TASK_REF_3NODES_TASK_LIST,
        )

        # Nodes
        self.assertEqual(sorted(TASK_REF_3NODES_TASK_LIST), sorted(nodes))

        # Edges
        expected_edges = [
            ('task01.Task01', 'task02.Task02'),
            ('task02.Task02', 'task03.Task03'),
        ]
        self.assertEqual(sorted(expected_edges), sorted(edges))

    def test_create_nodes_edges_5nodes(self):
        """
        Create nodes / edges for the DAG with 5 nodes.
        """
        nodes, edges, _ = self._create_nodes_edges(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_5NODES_MODULE_LIST,
            tasks_dir=TASK_REF_5NODES_DIR,
            task_list=TASK_REF_5NODES_TASK_LIST,
        )

        # Nodes
        self.assertEqual(sorted(TASK_REF_5NODES_TASK_LIST), sorted(nodes))

        # Edges
        expected_edges = [
            ('taskA.Taska', 'taskB.Taskb'),
            ('taskA.Taska', 'taskC.Taskc'),
            ('taskB.Taskb', 'taskD.Taskd'),
            ('taskA.Taska', 'taskD.Taskd'),
            ('taskC.Taskc', 'taskD.Taskd'),
            ('taskA.Taska', 'taskE.Taske'),
            ('taskC.Taskc', 'taskE.Taske'),
            ('taskD.Taskd', 'taskE.Taske'),
        ]
        self.assertEqual(sorted(expected_edges), sorted(edges))

    def test_create_nodes_edges_15nodes(self):
        """
        Create nodes / edges for the DAG with 3 nodes.
        """
        nodes, edges, _ = self._create_nodes_edges(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_15NODES_MODULE_LIST,
            tasks_dir=TASK_REF_15NODES_DIR,
            task_list=TASK_REF_15NODES_TASK_LIST,
        )

        # Nodes
        self.assertEqual(sorted(TASK_REF_15NODES_TASK_LIST), sorted(nodes))

        # Edges
        expected_edges = [
            ('task01.Task01', 'task02.Task02'),
            ('task01.Task01', 'task03.Task03'),
            ('task02.Task02', 'task04.Task04'),
            ('task03.Task03', 'task04.Task04'),
            ('task01.Task01', 'task05.Task05'),
            ('task05.Task05', 'task06.Task06'),
            ('task04.Task04', 'task07.Task07a'),
            ('task06.Task06', 'task07.Task07a'),
            ('task07.Task07a', 'task07.task_07b'),
            ('task01.Task01', 'task08.Task08'),
            ('task05.Task05', 'task09.Task09'),
            ('task08.Task08', 'task09.Task09'),
            ('task01.Task01', 'task10.Task10'),
            ('task07.Task07a', 'task11.Task11'),
            ('task10.Task10', 'task11.Task11'),
            ('task10.Task10', 'task12.Task12'),
            ('task10.Task10', 'task13.Task13'),
            ('task11.Task11', 'task14.Task14'),
            ('task11.Task11', 'task15.Task15'),
        ]
        self.assertEqual(sorted(expected_edges), sorted(edges))

    def test_create_nodes_edges_norefs(self):
        """
        Create nodes / edges for the DAG with 3 nodes.
        """
        nodes, edges, _ = self._create_nodes_edges(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_NOREFS_MODULE_LIST,
            tasks_dir=TASK_REF_NOREFS_DIR,
            task_list=TASK_REF_NOREFS_TASK_LIST,
        )

        # Nodes
        self.assertEqual(sorted(TASK_REF_NOREFS_TASK_LIST), sorted(nodes))

        # Edges
        expected_edges = []
        self.assertEqual(sorted(expected_edges), sorted(edges))

    def test_create_nodes_edges_cycle(self):
        """
        Create nodes / edges for the DAG with a cycle
        """
        nodes, edges, _ = self._create_nodes_edges(
            project_dir=DAG_TEST_CASES,
            module_list=DAG_CYCLE_MODULE_LIST,
            tasks_dir=DAG_CYCLE_DIR,
            task_list=DAG_CYCLE_TASK_LIST,
        )

        # Nodes
        self.assertEqual(sorted(DAG_CYCLE_TASK_LIST), sorted(nodes))

        # Edges
        expected_edges = [
            ('moduleA.Taska', 'moduleB.Taskb'),
            ('moduleE.Taske', 'moduleB.Taskb'),
            ('moduleA.Taska', 'moduleC.Taskc'),
            ('moduleC.Taskc', 'moduleD.Taskd'),
            ('moduleB.Taskb', 'moduleD.Taskd'),
            ('moduleA.Taska', 'moduleD.Taskd'),
            ('moduleD.Taskd', 'moduleE.Taske'),
            ('moduleA.Taska', 'moduleE.Taske'),
            ('moduleC.Taskc', 'moduleE.Taske'),
        ]
        self.assertEqual(sorted(expected_edges), sorted(edges))

    ##############
    # Create DAG #
    ##############

    def _create_dag(self,
        project_dir: Path,
        module_list: List[Any],
        tasks_dir: Path,
        task_list: List[str]
    ):
        nodes, edges, dag_compiler = self._create_nodes_edges(
            project_dir=project_dir,
            module_list=module_list,
            tasks_dir=tasks_dir,
            task_list=task_list,
        )
        dag = dag_compiler.create_dag(
            nodes, edges
        )
        return dag, dag_compiler

    def test_create_dag_varying_complexity(self):
        """
        Create topsort for DAGs with different node counts and dependency complexities.
        These should not result in an error.
        """
        self._create_dag(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_3NODES_MODULE_LIST,
            tasks_dir=TASK_REF_3NODES_DIR,
            task_list=TASK_REF_3NODES_TASK_LIST,
        )

        self._create_dag(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_5NODES_MODULE_LIST,
            tasks_dir=TASK_REF_5NODES_DIR,
            task_list=TASK_REF_5NODES_TASK_LIST,
        )

        self._create_dag(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_15NODES_MODULE_LIST,
            tasks_dir=TASK_REF_15NODES_DIR,
            task_list=TASK_REF_15NODES_TASK_LIST,
        )

        self._create_dag(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_NOREFS_MODULE_LIST,
            tasks_dir=TASK_REF_NOREFS_DIR,
            task_list=TASK_REF_NOREFS_TASK_LIST,
        )

    def test_create_dag_cycle(self):
        """
        Cycle in DAG throws an error
        """
        with self.assertRaises(prism.exceptions.DAGException) as cm:
            self._create_dag(
                project_dir=DAG_TEST_CASES,
                module_list=DAG_CYCLE_MODULE_LIST,
                tasks_dir=DAG_CYCLE_DIR,
                task_list=DAG_CYCLE_TASK_LIST,
            )
        expected_msg = "invalid DAG, cycle found"
        cycled_tasks = ['moduleD.Taskd', 'moduleB.Taskb', 'moduleE.Taske']
        self.assertTrue(expected_msg in str(cm.exception))
        for _t in cycled_tasks:
            self.assertTrue(str(_t) in str(cm.exception))

    #########################
    # Get node dependencies #
    #########################

    def test_node_dependencies_3nodes(self):
        """
        Get node dependencies from DAGs with 3 nodes.
        """
        dag, dag_compiler = self._create_dag(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_3NODES_MODULE_LIST,
            tasks_dir=TASK_REF_3NODES_DIR,
            task_list=TASK_REF_3NODES_TASK_LIST,
        )
        node_deps_1 = dag_compiler.get_node_dependencies(
            dag, ["task01.Task01"]
        )
        node_deps_2 = dag_compiler.get_node_dependencies(
            dag, ['task02.Task02']
        )
        node_deps_3 = dag_compiler.get_node_dependencies(
            dag, ['task03.Task03']
        )
        node_deps_1_2 = dag_compiler.get_node_dependencies(
            dag, ['task01.Task01', 'task02.Task02']
        )

        # Expected node dependencies
        expected_node_deps_1 = ['task01.Task01']
        expected_node_deps_2 = ['task01.Task01', 'task02.Task02']
        expected_node_deps_3 = ['task01.Task01', 'task02.Task02', 'task03.Task03']
        expected_node_deps_1_2 = ['task01.Task01', 'task02.Task02']

        # Asserts
        self.assertEqual(set(expected_node_deps_1), set(node_deps_1))
        self.assertEqual(set(expected_node_deps_2), set(node_deps_2))
        self.assertEqual(set(expected_node_deps_3), set(node_deps_3))
        self.assertEqual(set(expected_node_deps_1_2), set(node_deps_1_2))

    def test_node_dependencies_5nodes(self):
        """
        Get node dependencies from DAGs with 5 nodes.
        """
        dag, dag_compiler = self._create_dag(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_5NODES_MODULE_LIST,
            tasks_dir=TASK_REF_5NODES_DIR,
            task_list=TASK_REF_5NODES_TASK_LIST,
        )
        # Node dependencies
        node_deps_e = dag_compiler.get_node_dependencies(
            dag, ['taskE.Taske']
        )
        expected_node_deps_e = [
            'taskA.Taska',
            'taskB.Taskb',
            'taskC.Taskc',
            'taskD.Taskd',
            'taskE.Taske',
        ]
        self.assertEqual(set(expected_node_deps_e), set(node_deps_e))

    def test_node_dependencies_15nodes(self):
        """
        Get node dependencies from DAGs with 15 nodes.
        """
        dag, dag_compiler = self._create_dag(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_15NODES_MODULE_LIST,
            tasks_dir=TASK_REF_15NODES_DIR,
            task_list=TASK_REF_15NODES_TASK_LIST,
        )

        # Node dependencies
        node_deps_7b = dag_compiler.get_node_dependencies(
            dag, ['task07.task_07b']
        )
        expected_node_deps_7 = [
            'task07.task_07b',
            'task07.Task07a',
            'task04.Task04',
            'task06.Task06',
            'task02.Task02',
            'task03.Task03',
            'task05.Task05',
            'task01.Task01'
        ]
        self.assertEqual(set(expected_node_deps_7), set(node_deps_7b))

        # Set of nodes with all nodes as their dependencies
        node_deps_all = dag_compiler.get_node_dependencies(
            dag,
            [
                'task07.task_07b',
                'task14.Task14',
                'task15.Task15',
                'task12.Task12',
                'task13.Task13',
                'task09.Task09'
            ]
        )
        expected_node_deps_all = [
            'task01.Task01',
            'task02.Task02',
            'task03.Task03',
            'task04.Task04',
            'task05.Task05',
            'task06.Task06',
            'task07.Task07a',
            'task07.task_07b',
            'task08.Task08',
            'task09.Task09',
            'task10.Task10',
            'task11.Task11',
            'task12.Task12',
            'task13.Task13',
            'task14.Task14',
            'task15.Task15'
        ]
        self.assertEqual(set(expected_node_deps_all), set(node_deps_all))

    ##################
    # Create topsort #
    ##################

    def _create_topsort(self,
        project_dir: Path,
        module_list: List[Any],
        tasks_dir: Path,
        task_list: List[str],
        user_arg_tasks: Optional[List[str]] = None
    ):
        all_parsed_tasks = self.parse_all_tasks(
            module_list,
            tasks_dir=tasks_dir
        )
        _, dag_compiler = self._parse_task_refs(
            project_dir=project_dir,
            module_list=module_list,
            tasks_dir=tasks_dir,
            task_list=task_list
        )
        if user_arg_tasks is None:
            user_arg_tasks = task_list
        _, topsort = dag_compiler.create_topsort(
            all_tasks=task_list,
            user_arg_tasks=user_arg_tasks,
            parsed_tasks=all_parsed_tasks
        )
        return topsort

    def _parent_before_child(self,
        parent_list,
        parent: str,
        child: Union[str, list]
    ):
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
            self.assertTrue(idx1 < idx2)
        elif isinstance(child, list):
            for node in child:
                idx2 = parent_list.index(node)
                self.assertTrue(idx1 < idx2)

    def test_topsort_3nodes(self):
        """
        Create the topological sort for the DAG with 3 nodes
        """
        topsort = self._create_topsort(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_3NODES_MODULE_LIST,
            tasks_dir=TASK_REF_3NODES_DIR,
            task_list=TASK_REF_3NODES_TASK_LIST,
        )
        expected_topsort = [
            'task01.Task01',
            'task02.Task02',
            'task03.Task03'
        ]
        self.assertEqual(expected_topsort, topsort)

    def test_topsort_5nodes(self):
        """
        Create the topological sort for the DAG with 5 nodes
        """
        topsort = self._create_topsort(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_5NODES_MODULE_LIST,
            tasks_dir=TASK_REF_5NODES_DIR,
            task_list=TASK_REF_5NODES_TASK_LIST,
        )

        self._parent_before_child(
            topsort,
            'taskA.Taska',
            [
                'taskB.Taskb',
                'taskC.Taskc',
                'taskD.Taskd',
                'taskE.Taske'
            ],
        )
        self._parent_before_child(
            topsort,
            'taskB.Taskb',
            'taskD.Taskd',
        )
        self._parent_before_child(
            topsort,
            'taskC.Taskc',
            [
                'taskD.Taskd',
                'taskE.Taske'
            ],
        )
        self._parent_before_child(
            topsort,
            'taskD.Taskd',
            'taskE.Taske'
        )

    def test_topsort_15nodes(self):
        """
        Create the topological sort for the DAG with 15 nodes
        """
        topsort = self._create_topsort(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_15NODES_MODULE_LIST,
            tasks_dir=TASK_REF_15NODES_DIR,
            task_list=TASK_REF_15NODES_TASK_LIST,
        )

        self._parent_before_child(
            topsort,
            'task01.Task01',
            [
                'task10.Task10',
                'task02.Task02',
                'task03.Task03',
                'task05.Task05',
                'task08.Task08'
            ],
        )
        self._parent_before_child(
            topsort,
            'task10.Task10',
            ['task11.Task11', 'task12.Task12', 'task13.Task13'],
        )
        self._parent_before_child(
            topsort, 'task02.Task02', 'task04.Task04'
        )
        self._parent_before_child(
            topsort, 'task03.Task03', 'task04.Task04'
        )
        self._parent_before_child(
            topsort,
            'task05.Task05', ['task06.Task06', 'task09.Task09']
        )
        self._parent_before_child(
            topsort, 'task08.Task08', 'task09.Task09'
        )
        self._parent_before_child(
            topsort,
            'task11.Task11', ['task14.Task14', 'task15.Task15']
        )
        self._parent_before_child(
            topsort, 'task04.Task04', 'task07.Task07a'
        )
        self._parent_before_child(
            topsort, 'task06.Task06', 'task07.Task07a'
        )
        self._parent_before_child(
            topsort, 'task07.Task07a', 'task07.task_07b'
        )

    def test_topsort_no_refs(self):
        """
        DAGs with no dependencies should include all tasks (assuming a subset of
        tasks is not explicitly specified).
        """
        topsort = self._create_topsort(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_NOREFS_MODULE_LIST,
            tasks_dir=TASK_REF_NOREFS_DIR,
            task_list=TASK_REF_NOREFS_TASK_LIST,
        )

        # All tasks must be in DAG
        for mod in TASK_REF_NOREFS_TASK_LIST:
            self.assertTrue(mod in topsort)

    def test_topsort_subset(self):
        """
        DAGs created with only a subset of nodes are created normally / without error
        """
        topsort_15_7 = self._create_topsort(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_15NODES_MODULE_LIST,
            tasks_dir=TASK_REF_15NODES_DIR,
            task_list=TASK_REF_15NODES_TASK_LIST,
            user_arg_tasks=["task07.Task07a"]
        )
        topsort_15_all = self._create_topsort(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_15NODES_MODULE_LIST,
            tasks_dir=TASK_REF_15NODES_DIR,
            task_list=TASK_REF_15NODES_TASK_LIST,
            user_arg_tasks=[
                'task07.task_07b',
                'task14.Task14',
                'task15.Task15',
                'task12.Task12',
                'task13.Task13',
                'task09.Task09'
            ]
        )
        topsort_norefs = self._create_topsort(
            project_dir=DAG_TEST_CASES,
            module_list=TASK_REF_NOREFS_MODULE_LIST,
            tasks_dir=TASK_REF_NOREFS_DIR,
            task_list=TASK_REF_NOREFS_TASK_LIST,
            user_arg_tasks=[
                'moduleB.Taskb',
                'moduleC.Taskc',
            ]
        )
        # In the DAG with 15 nodes, specify which nodes should be included in the DAG
        # based on the tasks for compilation
        actual_topsort_15nodes_7 = [
            'task01.Task01',
            'task02.Task02',
            'task03.Task03',
            'task04.Task04',
            'task05.Task05',
            'task06.Task06',
            'task07.Task07a'
        ]
        actual_topsort_15nodes_all = TASK_REF_15NODES_TASK_LIST
        self.assertEqual(set(actual_topsort_15nodes_7), set(topsort_15_7))
        self.assertEqual(set(actual_topsort_15nodes_all), set(topsort_15_all))

        # In the DAG without mod refs, there aren't any edges. The only nodes in the DAG
        # will be taskB and taskC
        actual_topsort_no_modrefs = ['moduleB.Taskb', 'moduleC.Taskc']
        self.assertEqual(set(actual_topsort_no_modrefs), set(topsort_norefs))
