"""
Unit testing for parsing models with tasks created via decorated functions.

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
MODEL_TEST_CASES = Path(TEST_CASE_WKDIR) / 'test_models'


# Tasks with decorators
DEC_BAD_DEC_NO_PARENTHESES = Path('dec_bad_dec_no_parentheses.py')
DEC_BAD_RUN_EXTRA_ARG = Path('dec_bad_run_extra_arg.py')
DEC_BAD_RUN_MISSING_ARG = Path('dec_bad_run_missing_arg.py')
DEC_DIFF_DECORATOR_STRUCTURE = Path('dec_diff_decorator_structure.py')
DEC_MULTIPLE_PRISM_TASKS = Path('dec_multiple_prism_tasks.py')
DEC_NO_PRISM_TASK = Path('dec_no_prism_task.py')
DEC_ONE_PRISM_TASK = Path('dec_one_prism_task.py')
DEC_OTHER_FUNCTIONS = Path('dec_other_functions.py')
DEC_TASK_WITH_TARGET = Path('dec_task_with_target.py')
DEC_TASKS_REFS = Path('dec_tasks_refs.py')


##############################
# Test case class definition #
##############################

class TestModelDecParsing(unittest.TestCase):

    def test_one_prism_task(self):
        """
        Test behavior of parser with just one Prism task
        """
        # Prism task
        parser = ast_parser.AstParser(DEC_ONE_PRISM_TASK, MODEL_TEST_CASES)

        # Prism task name
        prism_task_fn = parser.get_prism_task_nodes(parser.classes, parser.bases)
        prism_task_name = prism_task_fn.name
        prism_task_fn_args = parser.get_func_args(prism_task_fn)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_task_functions()

        self.assertEqual("task_function", prism_task_name)
        self.assertEqual(1, num_prism_tasks)
        self.assertEqual(['tasks', 'hooks'], prism_task_fn_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([], parser.parse())

    def test_no_prism_task(self):
        """
        Test behavior of parser when model has no Prism task
        """
        parser = ast_parser.AstParser(DEC_NO_PRISM_TASK, MODEL_TEST_CASES)

        # Prism task name
        prism_task_fn = parser.get_prism_task_nodes(parser.classes, parser.bases)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_task_functions()

        self.assertIsNone(prism_task_fn)
        self.assertEqual(0, num_prism_tasks)

        # Calling `parse` should throw an error
        with self.assertRaises(prism.exceptions.ParserException) as cm:
            parser.parse()
        expected_msg = f"no PrismTask in `{str(DEC_NO_PRISM_TASK)}`"
        self.assertEqual(expected_msg, str(cm.exception))

    def test_multiple_prism_task(self):
        """
        Test behavior of parser when model has multiple Prism tasks
        """
        parser = ast_parser.AstParser(DEC_MULTIPLE_PRISM_TASKS, MODEL_TEST_CASES)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_task_functions()
        self.assertEqual(2, num_prism_tasks)

        # Prism task name -- it should return the first one
        with self.assertRaises(prism.exceptions.RuntimeException) as cm:
            _ = parser.get_prism_task_nodes(
                parser.classes, parser.bases
            )
        expected_msg = f"too many functions decorated with `@task` in {DEC_MULTIPLE_PRISM_TASKS}"  # noqa: E501
        self.assertEqual(expected_msg, str(cm.exception))

    def test_diff_decorator_structure(self):
        """
        Different @task decorator structure (i.e., import @prism.decorators.task())
        should not affect the behavior of the parser
        """
        # Prism task
        parser = ast_parser.AstParser(DEC_DIFF_DECORATOR_STRUCTURE, MODEL_TEST_CASES)

        # Prism task function
        prism_task_fn = parser.get_prism_task_nodes(parser.classes, parser.bases)
        prism_fn_name = prism_task_fn.name
        run_func_args = parser.get_func_args(prism_task_fn)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_task_functions()

        self.assertEqual("task_fn_different_decorator_structure", prism_fn_name)
        self.assertEqual(1, num_prism_tasks)
        self.assertEqual(['tasks', 'hooks'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([], parser.parse())

    def test_other_functions(self):
        """
        Presence of other, non-PrismTask classes should not affect the behavior of the
        parser
        """
        # Prism task
        parser = ast_parser.AstParser(DEC_OTHER_FUNCTIONS, MODEL_TEST_CASES)

        # Prism task name
        prism_task_fn = parser.get_prism_task_nodes(parser.classes, parser.bases)
        prism_task_name = prism_task_fn.name
        run_func_args = parser.get_func_args(prism_task_fn)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_task_functions()

        self.assertEqual("task_function", prism_task_name)
        self.assertEqual(1, num_prism_tasks)
        self.assertEqual(['tasks', 'hooks'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([], parser.parse())

    def test_task_with_target(self):
        """
        Presence of target does not affect parser behavior
        """
        # Prism task
        parser = ast_parser.AstParser(DEC_TASK_WITH_TARGET, MODEL_TEST_CASES)

        # Prism task name
        prism_task_fn = parser.get_prism_task_nodes(parser.classes, parser.bases)
        prism_task_name = prism_task_fn.name
        run_func_args = parser.get_func_args(prism_task_fn)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_task_functions()

        self.assertEqual("task_with_target", prism_task_name)
        self.assertEqual(1, num_prism_tasks)
        self.assertEqual(['tasks', 'hooks'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([Path('hello.py'), Path('world.py')], parser.parse())

        # Get target
        targets = parser.get_targets(prism_task_fn, prism_task_fn)
        expected_targets = "Path(__file__) / 'test.txt'"
        self.assertEqual(targets, expected_targets)

    def test_tasks_refs(self):
        """
        Test behavior of parse when there are mod references
        """
        # Prism task
        parser = ast_parser.AstParser(DEC_TASKS_REFS, MODEL_TEST_CASES)

        # Prism task name
        prism_task_fn = parser.get_prism_task_nodes(parser.classes, parser.bases)
        prism_task_name = prism_task_fn.name
        run_func_args = parser.get_func_args(prism_task_fn)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_task_functions()

        self.assertEqual("task_with_refs", prism_task_name)
        self.assertEqual(1, num_prism_tasks)
        self.assertEqual(['tasks', 'hooks'], run_func_args)

        # Calling `parse` shouldn't throw an error
        expected_tasks = [
            Path('helper_function1.py'),
            Path('helper_function2.py'),
            Path('hello.py'),
            Path('world.py')
        ]
        self.assertEqual(sorted(expected_tasks), sorted(parser.parse()))

    def test_bad_function_dec_definition(self):
        """
        Parser throws an error if:
            1. The function decorated with `@task` is not properly structured, or
            2. The @task decorator is not properly specified
        """

        def _get_args(model):
            parser = ast_parser.AstParser(model, MODEL_TEST_CASES)
            prism_task_fn = parser.get_prism_task_nodes(parser.classes, parser.bases)
            run_func_args = parser.get_func_args(prism_task_fn)
            return run_func_args

        # Extra arg
        run_func_args = _get_args(DEC_BAD_RUN_EXTRA_ARG)
        self.assertEqual(
            sorted(['tasks', 'hooks', 'other_arg']), sorted(run_func_args)
        )

        # Missing arg
        run_func_args = _get_args(DEC_BAD_RUN_MISSING_ARG)
        self.assertEqual(sorted(['tasks']), sorted(run_func_args))

        # @task de
        run_func_args = _get_args(DEC_BAD_DEC_NO_PARENTHESES)
        self.assertEqual(sorted(['tasks', 'hooks']), sorted(run_func_args))

        # Missing arguments raise a ParserException
        for model in [
            DEC_BAD_RUN_EXTRA_ARG, DEC_BAD_RUN_MISSING_ARG,
        ]:
            with self.assertRaises(prism.exceptions.ParserException) as cm:
                parser = ast_parser.AstParser(model, MODEL_TEST_CASES)
                parser.parse()
            expected_msg = f'invalid arguments in `run` function in PrismTask in {str(model)}; should only be `tasks`,`hooks`'  # noqa: E501
            self.assertEqual(expected_msg, str(cm.exception))

        # An improper @task decorator raises a RuntimeException
        with self.assertRaises(prism.exceptions.RuntimeException) as cm:
            parser = ast_parser.AstParser(DEC_BAD_DEC_NO_PARENTHESES, MODEL_TEST_CASES)
            parser.parse()
        expected_msg = "`task` decorator not properly specified...try adding parentheses to it, e.g., `@task()`"  # noqa: E501
        self.assertEqual(expected_msg, str(cm.exception))
