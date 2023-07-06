"""
Unit testing for parsing tasks with tasks created via the PrismTask class.

Table of Contents:
- Imports
- Test case directory and paths
- Test case class definition
"""

###########
# Imports #
###########

# Standard library imports
import os
from pathlib import Path
import unittest

# Prism imports
import prism.exceptions
import prism.parsers.ast_parser as ast_parser


#################################
# Test case directory and paths #
#################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TASK_TEST_CASES = Path(TEST_CASE_WKDIR) / 'test_tasks'

# Test cases related to imports
ONE_PRISM_TASK = Path('one_prism_task.py')
NO_PRISM_TASK = Path('no_prism_task.py')
MULTIPLE_PRISM_TASKS = Path('multiple_prism_tasks.py')
DIFF_IMPORT_STRUCTURE = Path('diff_import_structure.py')
OTHER_CLASSES = Path('other_classes.py')
IF_NAME_MAIN = Path('if_name_main.py')
TASKS_REFS = Path('tasks_refs.py')
TASK_WITH_TARGET = Path('task_with_target.py')
BAD_RUN_EXTRA_ARG = Path('bad_run_extra_arg.py')
BAD_RUN_MISSING_ARG = Path('bad_run_missing_arg.py')
BAD_RUN_NO_TASKS = Path('bad_run_no_tasks.py')
NO_RUN_FUNC = Path('no_run_func.py')


##############################
# Test case class definition #
##############################

class TestTaskClassParsing(unittest.TestCase):

    def test_one_prism_task(self):
        """
        Test behavior of parser with just one Prism task
        """
        # Prism task
        parser = ast_parser.AstParser(ONE_PRISM_TASK, TASK_TEST_CASES)

        # Prism task name
        prism_task_class = parser.get_prism_task_nodes(parser.classes, parser.bases)
        prism_task_name = prism_task_class.name

        # Run function
        run_func = parser.get_run_func(prism_task_class)
        run_func_args = parser.get_func_args(run_func)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_task_classes(parser.bases)

        self.assertEqual("NormalPrismTask", prism_task_name)
        self.assertEqual(1, num_prism_tasks)
        self.assertEqual(['self', 'tasks', 'hooks'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([], parser.parse())

    def test_no_prism_task(self):
        """
        Test behavior of parser when task has no Prism task
        """
        parser = ast_parser.AstParser(NO_PRISM_TASK, TASK_TEST_CASES)

        # Prism task name
        prism_task_class = parser.get_prism_task_nodes(parser.classes, parser.bases)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_task_classes(parser.bases)

        self.assertIsNone(prism_task_class)
        self.assertEqual(0, num_prism_tasks)

        # Calling `parse` should throw an error
        with self.assertRaises(prism.exceptions.ParserException) as cm:
            parser.parse()
        expected_msg = f"no PrismTask in `{str(NO_PRISM_TASK)}`"
        self.assertEqual(expected_msg, str(cm.exception))

    def test_multiple_prism_task(self):
        """
        Test behavior of parser when task has multiple Prism tasks
        """
        parser = ast_parser.AstParser(MULTIPLE_PRISM_TASKS, TASK_TEST_CASES)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_task_classes(parser.bases)
        self.assertEqual(2, num_prism_tasks)

        # Prism task name -- it should return the first one
        with self.assertRaises(prism.exceptions.RuntimeException) as cm:
            _ = parser.get_prism_task_nodes(
                parser.classes, parser.bases
            )
        expected_msg = f"too many PrismTasks in `{str(MULTIPLE_PRISM_TASKS)}`"
        self.assertEqual(expected_msg, str(cm.exception))

    def test_diff_import_structure(self):
        """
        Different PrismTask import structure (i.e., import prism.task.PrismTask) should
        not affect the behavior of the parser
        """
        # Prism task
        parser = ast_parser.AstParser(DIFF_IMPORT_STRUCTURE, TASK_TEST_CASES)

        # Prism task name
        prism_task_class = parser.get_prism_task_nodes(parser.classes, parser.bases)
        prism_task_name = prism_task_class.name

        # Run function
        run_func = parser.get_run_func(prism_task_class)
        run_func_args = parser.get_func_args(run_func)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_task_classes(parser.bases)

        self.assertEqual("DiffImportStructure", prism_task_name)
        self.assertEqual(1, num_prism_tasks)
        self.assertEqual(['self', 'tasks', 'hooks'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([], parser.parse())

    def test_other_classes(self):
        """
        Presence of other, non-PrismTask classes should not affect the behavior of the
        parser
        """
        # Prism task
        parser = ast_parser.AstParser(OTHER_CLASSES, TASK_TEST_CASES)

        # Prism task name
        prism_task_class = parser.get_prism_task_nodes(parser.classes, parser.bases)
        prism_task_name = prism_task_class.name

        # Run function
        run_func = parser.get_run_func(prism_task_class)
        run_func_args = parser.get_func_args(run_func)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_task_classes(parser.bases)

        self.assertEqual("OnlyPrismTask", prism_task_name)
        self.assertEqual(1, num_prism_tasks)
        self.assertEqual(['self', 'tasks', 'hooks'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([], parser.parse())

    def test_task_with_target(self):
        """
        Presence of target does not affect parser behavior
        """
        # Prism task
        parser = ast_parser.AstParser(TASK_WITH_TARGET, TASK_TEST_CASES)

        # Prism task name
        prism_task_class = parser.get_prism_task_nodes(parser.classes, parser.bases)
        prism_task_name = prism_task_class.name

        # Run function
        run_func = parser.get_run_func(prism_task_class)
        run_func_args = parser.get_func_args(run_func)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_task_classes(parser.bases)

        self.assertEqual("TaskWithTarget", prism_task_name)
        self.assertEqual(1, num_prism_tasks)
        self.assertEqual(['self', 'tasks', 'hooks'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([Path('hello.py'), Path('world.py')], parser.parse())

        # Get target
        targets = parser.get_targets(prism_task_class, run_func)
        expected_targets = "os.path.join(os.getcwd(), 'temp')"
        self.assertEqual(targets, expected_targets)

    def test_tasks_refs(self):
        """
        Test behavior of parse when there are mod references
        """
        # Prism task
        parser = ast_parser.AstParser(TASKS_REFS, TASK_TEST_CASES)

        # Prism task name
        prism_task_class = parser.get_prism_task_nodes(parser.classes, parser.bases)
        prism_task_name = prism_task_class.name

        # Run function
        run_func = parser.get_run_func(prism_task_class)
        run_func_args = parser.get_func_args(run_func)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_task_classes(parser.bases)

        self.assertEqual("TasksRefs", prism_task_name)
        self.assertEqual(1, num_prism_tasks)
        self.assertEqual(['self', 'tasks', 'hooks'], run_func_args)

        # Calling `parse` shouldn't throw an error
        expected_tasks = [
            Path('func_0.py'),
            Path('func_1.py'),
            Path('hello.py'),
            Path('world.py')
        ]
        self.assertEqual(sorted(expected_tasks), sorted(parser.parse()))

    def test_if_name_main(self):
        """
        If a task contains `if __name__ == '__main__'`, throw an error
        """
        with self.assertRaises(prism.exceptions.ParserException) as cm:
            _ = ast_parser.AstParser(IF_NAME_MAIN, TASK_TEST_CASES)
        expected_msg = f'found `if __name__ == "__main__"` in `{str(IF_NAME_MAIN)}`; all task-specific code should be placed in `run` method'  # noqa: E501
        self.assertEqual(expected_msg, str(cm.exception))

    def test_bad_runs(self):
        """
        Parser throws an error if `run` function is not properly structured or if `run`
        function does not exist
        """

        def _get_args(task):
            parser = ast_parser.AstParser(task, TASK_TEST_CASES)
            prism_task_class = parser.get_prism_task_nodes(parser.classes, parser.bases)
            run_func = parser.get_run_func(prism_task_class)
            run_func_args = parser.get_func_args(run_func)
            return run_func_args

        # Extra arg
        run_func_args = _get_args(BAD_RUN_EXTRA_ARG)
        self.assertEqual(
            sorted(['self', 'tasks', 'hooks', 'other_arg']), sorted(run_func_args)
        )

        # Missing arg
        run_func_args = _get_args(BAD_RUN_MISSING_ARG)
        self.assertEqual(sorted(['self', 'tasks']), sorted(run_func_args))

        # No tasks
        run_func_args = _get_args(BAD_RUN_NO_TASKS)
        self.assertEqual(sorted(['self', 'hooks', 'other_arg']), sorted(run_func_args))

        # No run function
        parser = ast_parser.AstParser(NO_RUN_FUNC, TASK_TEST_CASES)
        prism_task_class = parser.get_prism_task_nodes(parser.classes, parser.bases)
        run_func = parser.get_run_func(prism_task_class)
        self.assertIsNone(run_func)

        for task in [
            BAD_RUN_EXTRA_ARG, BAD_RUN_MISSING_ARG, BAD_RUN_NO_TASKS, NO_RUN_FUNC
        ]:
            with self.assertRaises(prism.exceptions.ParserException) as cm:
                parser = ast_parser.AstParser(task, TASK_TEST_CASES)
                parser.parse()
            if task == NO_RUN_FUNC:
                expected_msg = f"no `run` function in PrismTask in `{str(task)}`"
            else:
                expected_msg = f'invalid arguments in `run` function in PrismTask in {str(task)}; should only be `self`,`tasks`,`hooks`'  # noqa: E501
            self.assertEqual(expected_msg, str(cm.exception))
