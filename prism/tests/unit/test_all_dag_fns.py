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

    def test_task_refs_3nodes(self):
        """
        Parse task references for DAG with 3 nodes
        """
        all_parsed_tasks = self.parse_all_tasks(
            TASK_REF_3NODES_MODULE_LIST,
            tasks_dir=TASK_REF_3NODES_DIR
        )
        all_task_names = self.get_task_names(all_parsed_tasks)
        self.assertEqual(sorted(TASK_REF_3NODES_TASK_LIST), sorted(all_task_names))

        # Task references
        dag_compiler = compiler.DagCompiler(
            project_dir=DAG_TEST_CASES,
            tasks_dir=TASK_REF_3NODES_DIR,
            compiled_dir=None,
            all_tasks=all_task_names,
            parsed_tasks=all_parsed_tasks,
            user_arg_tasks=[],
            user_arg_all_downstream=False,
            project=None,
        )
        task_refs_3nodes = dag_compiler.parse_task_refs(
            tasks=all_task_names,
            parsed_tasks=all_parsed_tasks
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
        all_parsed_tasks = self.parse_all_tasks(
            TASK_REF_5NODES_MODULE_LIST,
            tasks_dir=TASK_REF_5NODES_DIR
        )
        all_task_names = self.get_task_names(all_parsed_tasks)
        self.assertEqual(sorted(TASK_REF_5NODES_TASK_LIST), sorted(all_task_names))

        # Task references
        dag_compiler = compiler.DagCompiler(
            project_dir=DAG_TEST_CASES,
            tasks_dir=TASK_REF_5NODES_DIR,
            compiled_dir=None,
            all_tasks=all_task_names,
            parsed_tasks=all_parsed_tasks,
            user_arg_tasks=[],
            user_arg_all_downstream=False,
            project=None,
        )
        task_refs_5nodes = dag_compiler.parse_task_refs(
            tasks=all_task_names,
            parsed_tasks=all_parsed_tasks
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
        all_parsed_tasks = self.parse_all_tasks(
            TASK_REF_15NODES_MODULE_LIST,
            tasks_dir=TASK_REF_15NODES_DIR
        )
        all_task_names = self.get_task_names(all_parsed_tasks)
        self.assertEqual(sorted(TASK_REF_15NODES_TASK_LIST), sorted(all_task_names))

        # Task references
        dag_compiler = compiler.DagCompiler(
            project_dir=DAG_TEST_CASES,
            tasks_dir=TASK_REF_15NODES_DIR,
            compiled_dir=None,
            all_tasks=all_task_names,
            parsed_tasks=all_parsed_tasks,
            user_arg_tasks=[],
            user_arg_all_downstream=False,
            project=None,
        )
        task_refs_15nodes = dag_compiler.parse_task_refs(
            tasks=all_task_names,
            parsed_tasks=all_parsed_tasks
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

    def test_no_task_refs(self):
        """
        Parse task references for DAG with 5 nodes but no task refs
        """
        all_parsed_tasks = self.parse_all_tasks(
            TASK_REF_NOREFS_MODULE_LIST,
            tasks_dir=TASK_REF_NOREFS_DIR
        )
        all_task_names = self.get_task_names(all_parsed_tasks)
        self.assertEqual(sorted(TASK_REF_NOREFS_TASK_LIST), sorted(all_task_names))

        # Task references
        dag_compiler = compiler.DagCompiler(
            project_dir=DAG_TEST_CASES,
            tasks_dir=TASK_REF_NOREFS_DIR,
            compiled_dir=None,
            all_tasks=all_task_names,
            parsed_tasks=all_parsed_tasks,
            user_arg_tasks=[],
            user_arg_all_downstream=False,
            project=None,
        )
        task_refs_5nodes = dag_compiler.parse_task_refs(
            tasks=all_task_names,
            parsed_tasks=all_parsed_tasks
        )
        expected_task_refs = {
            'moduleA.Taska': [],
            'moduleB.Taskb': [],
            'moduleC.Taskc': [],
            'moduleD.Taskd': [],
            'moduleE.Taske': [],
        }
        for k, _ in expected_task_refs.items():
            self.assertEqual(sorted(expected_task_refs[k]), sorted(task_refs_5nodes[k]))

    def test_self_ref(self):
        """
        Parse task references for DAG with 5 nodes but no task refs
        """
        all_parsed_tasks = self.parse_all_tasks(
            TASK_REF_SELFREF_MODULE_LIST,
            tasks_dir=TASK_REF_SELFREF_DIR
        )
        all_task_names = self.get_task_names(all_parsed_tasks)
        self.assertEqual(sorted(TASK_REF_SELFREF_TASK_LIST), sorted(all_task_names))

        # Task references
        dag_compiler = compiler.DagCompiler(
            project_dir=DAG_TEST_CASES,
            tasks_dir=TASK_REF_NOREFS_DIR,
            compiled_dir=None,
            all_tasks=all_task_names,
            parsed_tasks=all_parsed_tasks,
            user_arg_tasks=[],
            user_arg_all_downstream=False,
            project=None,
        )
        with self.assertRaises(prism.exceptions.ReferenceException) as cm:
            _ = dag_compiler.parse_task_refs(
                tasks=all_task_names,
                parsed_tasks=all_parsed_tasks
            )
        expected_msg = 'Are you trying to access a task in the same module? If so, use only the task name as your tasks.ref() argument and set `local = True`'  # noqa
        self.assertEqual(expected_msg, str(cm.exception))
