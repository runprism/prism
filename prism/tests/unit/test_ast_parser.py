"""
Unit testing for functions used to parse {{ mod }} references in the tasks and
construct the DAG.

Table of Contents:
- Imports
- Compile task instance to call functions
- Test case class definition
"""


###########
# Imports #
###########

# Standard library imports
import argparse
import ast
import unittest
from pathlib import Path

# Prism imports
import prism.exceptions
import prism.prism_logging
from prism.parsers import ast_parser

# Set up logger
args = argparse.Namespace()
args.log_level = "info"
prism.prism_logging.set_up_logger(args)


#########
# Paths #
#########

# Specific task imports
TASK_TEST_CASE_WKDIR = Path(__file__).parent
TASK_TEST_CASES = TASK_TEST_CASE_WKDIR / 'test_tasks'

# Test cases related to imports
ONE_PRISM_TASK = Path('cls_one_prism_task.py')
NO_PRISM_TASK = Path('cls_no_prism_task.py')
MULTIPLE_PRISM_TASKS = Path('cls_multiple_prism_tasks.py')
DIFF_IMPORT_STRUCTURE = Path('cls_diff_import_structure.py')
OTHER_CLASSES = Path('cls_other_classes.py')
IF_NAME_MAIN = Path('cls_if_name_main.py')
TASKS_REFS = Path('cls_tasks_refs.py')
TASK_WITH_TARGET = Path('cls_task_with_target.py')
BAD_RUN_EXTRA_ARG = Path('cls_bad_run_extra_arg.py')
BAD_RUN_MISSING_ARG = Path('cls_bad_run_missing_arg.py')
BAD_RUN_NO_TASKS = Path('cls_bad_run_no_tasks.py')
NO_RUN_FUNC = Path('cls_no_run_func.py')

# Tasks with decorators
DEC_ONE_PRISM_TASK = Path('dec_one_prism_task.py')
DEC_NO_PRISM_TASK = Path('dec_no_prism_task.py')
DEC_MULTIPLE_PRISM_TASKS = Path('dec_multiple_prism_tasks.py')
DEC_DIFF_DECORATOR_STRUCTURE = Path('dec_diff_decorator_structure.py')
DEC_OTHER_FUNCTIONS = Path('dec_other_functions.py')
DEC_TASKS_REFS = Path('dec_tasks_refs.py')
DEC_TASK_WITH_TARGET = Path('dec_task_with_target.py')
DEC_BAD_RUN_EXTRA_ARG = Path('dec_bad_run_extra_arg.py')
DEC_BAD_RUN_MISSING_ARG = Path('dec_bad_run_missing_arg.py')
DEC_BAD_DEC_NO_PARENTHESES = Path('dec_bad_dec_no_parentheses.py')


##############################
# Test case class definition #
##############################

class TestAstParserClassTasks(unittest.TestCase):

    def test_one_prism_task(self):
        """
        Test behavior of parser with just one Prism task
        """
        # Prism task
        parser = ast_parser.AstParser(ONE_PRISM_TASK, TASK_TEST_CASES)

        # Prism task number, name, and type
        prism_task_nodes = parser.prism_task_nodes
        prism_task_names = parser.prism_task_names
        self.assertEqual(1, len(prism_task_nodes))
        self.assertEqual(1, len(prism_task_names))
        prism_task = prism_task_nodes[0]
        prism_task_name = prism_task_names[0]
        self.assertTrue(isinstance(prism_task, ast.ClassDef))
        self.assertEqual("NormalPrismTask", prism_task.name)
        self.assertEqual("NormalPrismTask", prism_task_name)

        # Run function
        run_func = parser.get_run_func(prism_task)
        run_func_args = parser.get_func_args(run_func)
        self.assertEqual(['self', 'tasks', 'hooks'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([], parser.parse(prism_task_name, []))

    def test_no_prism_task(self):
        """
        Test behavior of parser when task has no Prism task
        """
        parser = ast_parser.AstParser(NO_PRISM_TASK, TASK_TEST_CASES)

        # Prism task number, name, and type
        prism_task_nodes = parser.prism_task_nodes
        prism_task_names = parser.prism_task_names
        self.assertEqual(0, len(prism_task_nodes))
        self.assertEqual(0, len(prism_task_names))

        # Calling `parse` should throw an error
        with self.assertRaises(prism.exceptions.ParserException) as cm:
            parser.parse("", [])
        expected_msg = f"no PrismTask in `{str(NO_PRISM_TASK)}`"
        self.assertEqual(expected_msg, str(cm.exception))

    def test_multiple_prism_task(self):
        """
        Test behavior of parser when task has multiple Prism tasks
        """
        parser = ast_parser.AstParser(MULTIPLE_PRISM_TASKS, TASK_TEST_CASES)

        # Prism task number, name, and type
        prism_task_nodes = parser.prism_task_nodes
        prism_task_names = parser.prism_task_names
        self.assertEqual(2, len(prism_task_nodes))
        self.assertEqual(2, len(prism_task_names))

        # First task
        first_prism_task = prism_task_nodes[0]
        first_prism_task_name = prism_task_names[0]
        self.assertTrue(isinstance(first_prism_task, ast.ClassDef))
        self.assertEqual("FirstPrismTask", first_prism_task.name)
        self.assertEqual("FirstPrismTask", first_prism_task_name)

        # Second task
        second_prism_task = prism_task_nodes[1]
        second_prism_task_name = prism_task_names[1]
        self.assertTrue(isinstance(second_prism_task, ast.ClassDef))
        self.assertEqual("SecondPrismTask", second_prism_task.name)
        self.assertEqual("SecondPrismTask", second_prism_task_name)

        # Call parse on the different tasks
        self.assertEqual([], parser.parse(first_prism_task_name, []))
        self.assertEqual([], parser.parse(second_prism_task_name, []))

    def test_diff_import_structure(self):
        """
        Different PrismTask import structure (i.e., import prism.task.PrismTask) should
        not affect the behavior of the parser
        """
        # Prism task
        parser = ast_parser.AstParser(DIFF_IMPORT_STRUCTURE, TASK_TEST_CASES)

        # Prism task number, name, and type
        prism_task_nodes = parser.prism_task_nodes
        prism_task_names = parser.prism_task_names
        self.assertEqual(1, len(prism_task_nodes))
        self.assertEqual(1, len(prism_task_names))
        prism_task = prism_task_nodes[0]
        prism_task_name = prism_task_names[0]
        self.assertTrue(isinstance(prism_task, ast.ClassDef))
        self.assertEqual("DiffImportStructure", prism_task.name)
        self.assertEqual("DiffImportStructure", prism_task_name)

        # Run function
        run_func = parser.get_run_func(prism_task)
        run_func_args = parser.get_func_args(run_func)
        self.assertEqual(['self', 'tasks', 'hooks'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([], parser.parse(prism_task_name, []))

    def test_other_classes(self):
        """
        Presence of other, non-PrismTask classes should not affect the behavior of the
        parser
        """
        # Prism task
        parser = ast_parser.AstParser(OTHER_CLASSES, TASK_TEST_CASES)

        # Prism task number, name, and type
        prism_task_nodes = parser.prism_task_nodes
        prism_task_names = parser.prism_task_names
        self.assertEqual(1, len(prism_task_nodes))
        self.assertEqual(1, len(prism_task_names))
        prism_task = prism_task_nodes[0]
        prism_task_name = prism_task_names[0]
        self.assertTrue(isinstance(prism_task, ast.ClassDef))
        self.assertEqual("OnlyPrismTask", prism_task.name)
        self.assertEqual("OnlyPrismTask", prism_task_name)

        # Run function
        run_func = parser.get_run_func(prism_task)
        run_func_args = parser.get_func_args(run_func)
        self.assertEqual(['self', 'tasks', 'hooks'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([], parser.parse(prism_task_name, []))

    def test_task_with_target(self):
        """
        Presence of target does not affect parser behavior
        """
        # Prism task
        parser = ast_parser.AstParser(TASK_WITH_TARGET, TASK_TEST_CASES)

        # Prism task number, name, and type
        prism_task_nodes = parser.prism_task_nodes
        prism_task_names = parser.prism_task_names
        self.assertEqual(1, len(prism_task_nodes))
        self.assertEqual(1, len(prism_task_names))
        prism_task = prism_task_nodes[0]
        prism_task_name = prism_task_names[0]
        self.assertTrue(isinstance(prism_task, ast.ClassDef))
        self.assertEqual("TaskWithTarget", prism_task.name)
        self.assertEqual("TaskWithTarget", prism_task_name)

        # Run function
        run_func = parser.get_run_func(prism_task)
        run_func_args = parser.get_func_args(run_func)
        self.assertEqual(['self', 'tasks', 'hooks'], run_func_args)

        # Get the other ast parsers (for `parse`)
        hello_parser = ast_parser.AstParser(
            Path('hello.py'), TASK_TEST_CASES / 'refd_tasks'
        )
        world_parser = ast_parser.AstParser(
            Path('world.py'), TASK_TEST_CASES / 'refd_tasks'
        )
        parsed_refs = parser.parse(
            "TaskWithTarget",
            [hello_parser, world_parser]
        )
        expected_parsed_refs = [
            "hello.Hello", "world.World"
        ]
        self.assertEqual(sorted(expected_parsed_refs), sorted(parsed_refs))

        # Get target
        targets = parser.get_targets(prism_task, run_func)
        expected_targets = "os.path.join(os.getcwd(), 'temp')"
        self.assertEqual(targets, expected_targets)

    def test_tasks_refs(self):
        """
        Test behavior of parse when there are mod references
        """
        # Prism task
        parser = ast_parser.AstParser(TASKS_REFS, TASK_TEST_CASES)

        # Prism task number, name, and type
        prism_task_nodes = parser.prism_task_nodes
        prism_task_names = parser.prism_task_names
        self.assertEqual(1, len(prism_task_nodes))
        self.assertEqual(1, len(prism_task_names))
        prism_task = prism_task_nodes[0]
        prism_task_name = prism_task_names[0]
        self.assertTrue(isinstance(prism_task, ast.ClassDef))
        self.assertEqual("TasksRefs", prism_task.name)
        self.assertEqual("TasksRefs", prism_task_name)

        # Run function
        run_func = parser.get_run_func(prism_task)
        run_func_args = parser.get_func_args(run_func)
        self.assertEqual(['self', 'tasks', 'hooks'], run_func_args)

        # Get the other ast parsers (for `parse`)
        hello_parser = ast_parser.AstParser(
            Path('hello.py'), TASK_TEST_CASES / 'refd_tasks'
        )
        world_parser = ast_parser.AstParser(
            Path('world.py'), TASK_TEST_CASES / 'refd_tasks'
        )
        func0_parser = ast_parser.AstParser(
            Path('func_0.py'), TASK_TEST_CASES / 'refd_tasks'
        )
        func1_parser = ast_parser.AstParser(
            Path('func_1.py'), TASK_TEST_CASES / 'refd_tasks'
        )
        parsed_refs = parser.parse(
            "TasksRefs",
            [hello_parser, world_parser, func0_parser, func1_parser]
        )
        expected_parsed_refs = [
            "hello.Hello", "world.World", "func_0.Func0", "func_1.Func1",
        ]
        self.assertEqual(sorted(expected_parsed_refs), sorted(parsed_refs))

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
            prism_task_nodes = parser.prism_task_nodes
            prism_task_names = parser.prism_task_names
            self.assertEqual(1, len(prism_task_nodes))
            self.assertEqual(1, len(prism_task_names))

            # Get Prism task
            prism_task = prism_task_nodes[0]
            run_func = parser.get_run_func(prism_task)
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
        prism_task_nodes = parser.prism_task_nodes
        prism_task_names = parser.prism_task_names
        self.assertEqual(1, len(prism_task_nodes))
        self.assertEqual(1, len(prism_task_names))
        run_func = parser.get_run_func(prism_task_nodes[0])
        self.assertIsNone(run_func)

        for task_dir, task_name in zip(
            [BAD_RUN_EXTRA_ARG, BAD_RUN_MISSING_ARG, BAD_RUN_NO_TASKS, NO_RUN_FUNC],
            ["BadRunExtraArg", "BadRunMissingArg", "BadRunNoTasks", "NoRunFunc"]
        ):
            with self.assertRaises(prism.exceptions.ParserException) as cm:
                parser = ast_parser.AstParser(task_dir, TASK_TEST_CASES)
                parser.parse(task_name, [])
            if task_dir == NO_RUN_FUNC:
                expected_msg = f"no `run` function in PrismTask in `{task_dir}`"
            else:
                expected_msg = f'invalid arguments in `run` function in PrismTask in `{task_dir}.{task_name}`; should only be `self`,`tasks`,`hooks`'  # noqa: E501
            self.assertEqual(expected_msg, str(cm.exception))


class TestAstParserDecTasks(unittest.TestCase):

    def test_dec_one_prism_task(self):
        """
        Test behavior of parser with just one Prism task
        """
        # Prism task
        parser = ast_parser.AstParser(DEC_ONE_PRISM_TASK, TASK_TEST_CASES)

        # Prism task number, name, and type
        prism_task_nodes = parser.prism_task_nodes
        prism_task_names = parser.prism_task_names
        self.assertEqual(1, len(prism_task_nodes))
        self.assertEqual(1, len(prism_task_names))
        prism_task = prism_task_nodes[0]
        prism_task_name = prism_task_names[0]
        self.assertTrue(isinstance(prism_task, ast.FunctionDef))
        self.assertEqual("task_function", prism_task.name)
        self.assertEqual("task_function", prism_task_name)

        # Run function
        run_func = parser.get_run_func(prism_task)
        run_func_args = parser.get_func_args(run_func)
        self.assertEqual(['tasks', 'hooks'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([], parser.parse(prism_task_name, []))

    def test_dec_no_prism_task(self):
        """
        Test behavior of parser when task has no Prism task
        """
        parser = ast_parser.AstParser(DEC_NO_PRISM_TASK, TASK_TEST_CASES)

        # Prism task number, name, and type
        prism_task_nodes = parser.prism_task_nodes
        prism_task_names = parser.prism_task_names
        self.assertEqual(0, len(prism_task_nodes))
        self.assertEqual(0, len(prism_task_names))

        # Calling `parse` should throw an error
        with self.assertRaises(prism.exceptions.ParserException) as cm:
            parser.parse("", [])
        expected_msg = f"no PrismTask in `{str(DEC_NO_PRISM_TASK)}`"
        self.assertEqual(expected_msg, str(cm.exception))

    def test_dec_multiple_prism_task(self):
        """
        Test behavior of parser when task has multiple Prism tasks
        """
        parser = ast_parser.AstParser(DEC_MULTIPLE_PRISM_TASKS, TASK_TEST_CASES)

        # Prism task number, name, and type
        prism_task_nodes = parser.prism_task_nodes
        prism_task_names = parser.prism_task_names
        self.assertEqual(2, len(prism_task_nodes))
        self.assertEqual(2, len(prism_task_names))

        # First task
        first_prism_task = prism_task_nodes[0]
        first_prism_task_name = prism_task_names[0]
        self.assertTrue(isinstance(first_prism_task, ast.FunctionDef))
        self.assertEqual("task_function_1", first_prism_task.name)
        self.assertEqual("task_function_1", first_prism_task_name)

        # Second task
        second_prism_task = prism_task_nodes[1]
        second_prism_task_name = prism_task_names[1]
        self.assertTrue(isinstance(second_prism_task, ast.FunctionDef))
        self.assertEqual("task_function_2", second_prism_task.name)
        self.assertEqual("task_function_2", second_prism_task_name)

        # Call parse on the different tasks
        self.assertEqual([], parser.parse(first_prism_task_name, []))
        self.assertEqual([], parser.parse(second_prism_task_name, []))

    def test_dec_diff_decorator_structure(self):
        """
        Different @task decorator structure (i.e., import @prism.decorators.task())
        should not affect the behavior of the parser
        """
        # Prism task
        parser = ast_parser.AstParser(DEC_DIFF_DECORATOR_STRUCTURE, TASK_TEST_CASES)

        # Prism task number, name, and type
        prism_task_nodes = parser.prism_task_nodes
        prism_task_names = parser.prism_task_names
        self.assertEqual(1, len(prism_task_nodes))
        self.assertEqual(1, len(prism_task_names))
        prism_task = prism_task_nodes[0]
        prism_task_name = prism_task_names[0]
        self.assertTrue(isinstance(prism_task, ast.FunctionDef))
        self.assertEqual("task_fn_different_decorator_structure", prism_task.name)
        self.assertEqual("task_fn_different_decorator_structure", prism_task_name)

        # Run function
        run_func = parser.get_run_func(prism_task)
        run_func_args = parser.get_func_args(run_func)
        self.assertEqual(['tasks', 'hooks'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([], parser.parse(prism_task_name, []))

    def test_dec_other_functions(self):
        """
        Presence of other, non-PrismTask classes should not affect the behavior of the
        parser
        """
        # Prism task
        parser = ast_parser.AstParser(DEC_OTHER_FUNCTIONS, TASK_TEST_CASES)

        # Prism task number, name, and type
        prism_task_nodes = parser.prism_task_nodes
        prism_task_names = parser.prism_task_names
        self.assertEqual(1, len(prism_task_nodes))
        self.assertEqual(1, len(prism_task_names))
        prism_task = prism_task_nodes[0]
        prism_task_name = prism_task_names[0]
        self.assertTrue(isinstance(prism_task, ast.FunctionDef))
        self.assertEqual("task_function", prism_task.name)
        self.assertEqual("task_function", prism_task_name)

        # Run function
        run_func = parser.get_run_func(prism_task)
        run_func_args = parser.get_func_args(run_func)
        self.assertEqual(['tasks', 'hooks'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([], parser.parse(prism_task_name, []))

    def test_dec_task_with_target(self):
        """
        Presence of target does not affect parser behavior
        """
        # Prism task
        parser = ast_parser.AstParser(DEC_TASK_WITH_TARGET, TASK_TEST_CASES)

        # Prism task number, name, and type
        prism_task_nodes = parser.prism_task_nodes
        prism_task_names = parser.prism_task_names
        self.assertEqual(1, len(prism_task_nodes))
        self.assertEqual(1, len(prism_task_names))
        prism_task = prism_task_nodes[0]
        prism_task_name = prism_task_names[0]
        self.assertTrue(isinstance(prism_task, ast.FunctionDef))
        self.assertEqual("task_with_target", prism_task.name)
        self.assertEqual("task_with_target", prism_task_name)

        # Run function
        run_func = parser.get_run_func(prism_task)
        run_func_args = parser.get_func_args(run_func)
        self.assertEqual(['tasks', 'hooks'], run_func_args)

        # Get the other ast parsers (for `parse`)
        hello_parser = ast_parser.AstParser(
            Path('hello.py'), TASK_TEST_CASES / 'refd_tasks'
        )
        world_parser = ast_parser.AstParser(
            Path('world.py'), TASK_TEST_CASES / 'refd_tasks'
        )
        parsed_refs = parser.parse(
            "task_with_target",
            [hello_parser, world_parser]
        )
        expected_parsed_refs = [
            "hello.Hello", "world.World"
        ]
        self.assertEqual(sorted(expected_parsed_refs), sorted(parsed_refs))

        # Get target
        targets = parser.get_targets(prism_task, run_func)
        expected_targets = "Path(__file__) / 'test.txt'"
        self.assertEqual(targets, expected_targets)

    def test_dec_tasks_refs(self):
        """
        Test behavior of parse when there are mod references
        """
        # Prism task
        parser = ast_parser.AstParser(DEC_TASKS_REFS, TASK_TEST_CASES)

        # Prism task number, name, and type
        prism_task_nodes = parser.prism_task_nodes
        prism_task_names = parser.prism_task_names
        self.assertEqual(1, len(prism_task_nodes))
        self.assertEqual(1, len(prism_task_names))
        prism_task = prism_task_nodes[0]
        prism_task_name = prism_task_names[0]
        self.assertTrue(isinstance(prism_task, ast.FunctionDef))
        self.assertEqual("task_with_refs", prism_task.name)
        self.assertEqual("task_with_refs", prism_task_name)

        # Run function
        run_func = parser.get_run_func(prism_task)
        run_func_args = parser.get_func_args(run_func)
        self.assertEqual(['tasks', 'hooks'], run_func_args)

        # Get the other ast parsers (for `parse`)
        hello_parser = ast_parser.AstParser(
            Path('hello.py'), TASK_TEST_CASES / 'refd_tasks'
        )
        world_parser = ast_parser.AstParser(
            Path('world.py'), TASK_TEST_CASES / 'refd_tasks'
        )
        func0_parser = ast_parser.AstParser(
            Path('func_0.py'), TASK_TEST_CASES / 'refd_tasks'
        )
        func1_parser = ast_parser.AstParser(
            Path('func_1.py'), TASK_TEST_CASES / 'refd_tasks'
        )
        parsed_refs = parser.parse(
            "task_with_refs",
            [hello_parser, world_parser, func0_parser, func1_parser]
        )
        expected_parsed_refs = [
            "hello.Hello", "world.World", "func_0.Func0", "func_1.Func1",
        ]
        self.assertEqual(sorted(expected_parsed_refs), sorted(parsed_refs))

    def test_bad_function_dec_definition(self):
        """
        Parser throws an error if:
            1. The function decorated with `@task` is not properly structured, or
            2. The @task decorator is not properly specified
        """

        def _get_args(task):
            parser = ast_parser.AstParser(task, TASK_TEST_CASES)
            prism_task_nodes = parser.prism_task_nodes
            prism_task_names = parser.prism_task_names
            self.assertEqual(1, len(prism_task_nodes))
            self.assertEqual(1, len(prism_task_names))
            prism_task = prism_task_nodes[0]
            run_func_args = parser.get_func_args(prism_task)
            return parser, run_func_args

        # Extra arg
        bad_run_extra_arg_parser, run_func_args = _get_args(DEC_BAD_RUN_EXTRA_ARG)
        self.assertEqual(
            sorted(['tasks', 'hooks', 'other_arg']), sorted(run_func_args)
        )

        # Missing arg
        bad_run_missing_arg_parser, run_func_args = _get_args(DEC_BAD_RUN_MISSING_ARG)
        self.assertEqual(sorted(['tasks']), sorted(run_func_args))

        # Missing parentheses
        bad_run_no_paren_parser, run_func_args = _get_args(DEC_BAD_DEC_NO_PARENTHESES)
        self.assertEqual(sorted(['tasks', 'hooks']), sorted(run_func_args))

        # Missing arguments raise a ParserException
        for parser in [bad_run_extra_arg_parser, bad_run_missing_arg_parser]:
            # Task name / path
            task_name = str(parser.prism_task_names[0])
            task_path = str(parser.task_relative_path)

            # Check exception
            with self.assertRaises(prism.exceptions.ParserException) as cm:
                parser.parse(parser.prism_task_names[0], [])
            expected_msg = f'invalid arguments in `run` function in PrismTask in `{task_path}.{task_name}`; should only be `tasks`,`hooks`'  # noqa: E501
            self.assertEqual(expected_msg, str(cm.exception))

        # An improper @task decorator raises a RuntimeException
        with self.assertRaises(prism.exceptions.RuntimeException) as cm:
            task_name = str(bad_run_no_paren_parser.prism_task_names[0])
            bad_run_no_paren_parser.parse(
                task_name,
                []
            )
        expected_msg = "`task` decorator not properly specified...try adding parentheses to it, e.g., `@task()`"  # noqa: E501
        self.assertEqual(expected_msg, str(cm.exception))
