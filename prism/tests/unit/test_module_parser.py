"""
Unit testing for functions for PythonModuleParser class.

Table of Contents:
- Imports
- Test case directory and paths
- Test case class definition
"""

#############
## Imports ##
#############

# Standard library imports
import os
from pathlib import Path
import unittest

# Prism imports
import prism.exceptions
import prism.parsers.ast_parser as ast_parser


###################################
## Test case directory and paths ##
###################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
MODULE_TEST_CASES = Path(TEST_CASE_WKDIR) / 'test_modules'

# Test cases related to imports
ONE_PRISM_TASK = Path('one_prism_task.py')
NO_PRISM_TASK = Path('no_prism_task.py')
MULTIPLE_PRISM_TASKS = Path('multiple_prism_tasks.py')
DIFF_IMPORT_STRUCTURE = Path('diff_import_structure.py')
OTHER_CLASSES = Path('other_classes.py')
IF_NAME_MAIN = Path('if_name_main.py')
PSM_MOD = Path('psm_mod.py')
TASK_WITH_TARGET = Path('task_with_target.py')
BAD_RUN_EXTRA_ARG = Path('bad_run_extra_arg.py')
BAD_RUN_MISSING_ARG = Path('bad_run_missing_arg.py')
BAD_RUN_NO_PSM = Path('bad_run_no_psm.py')
NO_RUN_FUNC = Path('no_run_func.py')



################################
## Test case class definition ##
################################

class TestModuleParsing(unittest.TestCase):

    def test_one_prism_task(self):
        """
        Test behavior of parser with just one Prism task
        """
        # Prism task
        parser = ast_parser.AstParser(ONE_PRISM_TASK, MODULE_TEST_CASES)
        
        # Prism task name
        prism_task_class = parser.get_prism_task_node(parser.classes, parser.bases)
        prism_task_name = prism_task_class.name

        # Run function
        run_func = parser.get_run_func(prism_task_class)
        run_func_args = parser.get_func_args(run_func)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_tasks(parser.bases)

        self.assertEqual("NormalPrismTask", prism_task_name)
        self.assertEqual(1, num_prism_tasks)
        self.assertEqual(['self', 'psm'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([], parser.parse())

    
    def test_no_prism_task(self):
        """
        Test behavior of parser when module has no Prism task
        """
        parser = ast_parser.AstParser(NO_PRISM_TASK, MODULE_TEST_CASES)

        # Prism task name
        prism_task_class = parser.get_prism_task_node(parser.classes, parser.bases)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_tasks(parser.bases)
        
        self.assertIsNone(prism_task_class)
        self.assertEqual(0, num_prism_tasks)

        # Calling `parse` should throw an error
        with self.assertRaises(prism.exceptions.ParserException) as cm:
            parser.parse()
        expected_msg = f"no PrismTask in `{str(NO_PRISM_TASK)}`"
        self.assertEqual(expected_msg, str(cm.exception))


    def test_multiple_prism_task(self):
        """
        Test behavior of parser when module has multiple Prism tasks
        """
        parser = ast_parser.AstParser(MULTIPLE_PRISM_TASKS, MODULE_TEST_CASES)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_tasks(parser.bases)

        # Prism task name -- it should return the first one
        first_prism_task_class = parser.get_prism_task_node(parser.classes, parser.bases)
        first_prism_task_class_name = first_prism_task_class.name

        # Run function
        run_func = parser.get_run_func(first_prism_task_class)
        run_func_args = parser.get_func_args(run_func)
        
        self.assertEqual(2, num_prism_tasks)
        self.assertEqual("FirstPrismTask", first_prism_task_class_name)
        self.assertEqual(['self', 'psm'], run_func_args)

        # Calling `parse` should throw an error
        with self.assertRaises(prism.exceptions.ParserException) as cm:
            parser.parse()
        expected_msg = f"too many PrismTasks in `{str(MULTIPLE_PRISM_TASKS)}`"
        self.assertEqual(expected_msg, str(cm.exception))
    

    def test_diff_import_structure(self):
        """
        Different PrismTask import structure (i.e., import prism.task.PrismTask) should not affect the behavior 
        of the parser
        """
        # Prism task
        parser = ast_parser.AstParser(DIFF_IMPORT_STRUCTURE, MODULE_TEST_CASES)
        
        # Prism task name
        prism_task_class = parser.get_prism_task_node(parser.classes, parser.bases)
        prism_task_name = prism_task_class.name

        # Run function
        run_func = parser.get_run_func(prism_task_class)
        run_func_args = parser.get_func_args(run_func)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_tasks(parser.bases)

        self.assertEqual("DiffImportStructure", prism_task_name)
        self.assertEqual(1, num_prism_tasks)
        self.assertEqual(['self', 'psm'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([], parser.parse())

    
    def test_other_classes(self):
        """
        Presence of other, non-PrismTask classes should not affect the behavior of the parser
        """
        # Prism task
        parser = ast_parser.AstParser(OTHER_CLASSES, MODULE_TEST_CASES)
        
        # Prism task name
        prism_task_class = parser.get_prism_task_node(parser.classes, parser.bases)
        prism_task_name = prism_task_class.name

        # Run function
        run_func = parser.get_run_func(prism_task_class)
        run_func_args = parser.get_func_args(run_func)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_tasks(parser.bases)

        self.assertEqual("OnlyPrismTask", prism_task_name)
        self.assertEqual(1, num_prism_tasks)
        self.assertEqual(['self', 'psm'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([], parser.parse())
    

    def test_task_with_target(self):
        """
        Presence of target does not affect parser behavior
        """
        # Prism task
        parser = ast_parser.AstParser(TASK_WITH_TARGET, MODULE_TEST_CASES)
        
        # Prism task name
        prism_task_class = parser.get_prism_task_node(parser.classes, parser.bases)
        prism_task_name = prism_task_class.name

        # Run function
        run_func = parser.get_run_func(prism_task_class)
        run_func_args = parser.get_func_args(run_func)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_tasks(parser.bases)

        self.assertEqual("TaskWithTarget", prism_task_name)
        self.assertEqual(1, num_prism_tasks)
        self.assertEqual(['self', 'psm'], run_func_args)

        # Calling `parse` shouldn't throw an error
        self.assertEqual([Path('hello.py'), Path('world.py')], parser.parse())
    

    def test_psm_mod(self):
        """
        Test behavior of parse when there is a mod reference
        """
        # Prism task
        parser = ast_parser.AstParser(PSM_MOD, MODULE_TEST_CASES)
        
        # Prism task name
        prism_task_class = parser.get_prism_task_node(parser.classes, parser.bases)
        prism_task_name = prism_task_class.name

        # Run function
        run_func = parser.get_run_func(prism_task_class)
        run_func_args = parser.get_func_args(run_func)

        # Number of prism tasks
        num_prism_tasks = parser.get_num_prism_tasks(parser.bases)

        self.assertEqual("PsmMod", prism_task_name)
        self.assertEqual(1, num_prism_tasks)
        self.assertEqual(['self', 'psm'], run_func_args)

        # Calling `parse` shouldn't throw an error
        expected_mods = [
            Path('func_0.py'),
            Path('func_1.py'),
            Path('hello.py'),
            Path('world.py')
        ]
        self.assertEqual(sorted(expected_mods), sorted(parser.parse()))
    

    def test_if_name_main(self):
        """
        If a module contains `if __name__ == '__main__'`, throw an error
        """
        with self.assertRaises(prism.exceptions.ParserException) as cm:
            parser = ast_parser.AstParser(IF_NAME_MAIN, MODULE_TEST_CASES)
        expected_msg_list = [
            f'found `if __name__=="__main__"` in `{str(IF_NAME_MAIN)}`',
            'all task-specific code should be placed in `run` method',
            'please fix and try again'
        ]
        self.assertEqual('\n'.join(expected_msg_list), str(cm.exception))

    
    def test_bad_runs(self):
        """
        Parser throws an error if `run` function is not properly structured or if `run` function does not exist
        """
        modules = [
            BAD_RUN_EXTRA_ARG,
            BAD_RUN_MISSING_ARG,
            NO_RUN_FUNC
        ]

        def _get_args(module):
            parser = ast_parser.AstParser(module, MODULE_TEST_CASES)
            prism_task_class = parser.get_prism_task_node(parser.classes, parser.bases)
            run_func = parser.get_run_func(prism_task_class)
            run_func_args = parser.get_func_args(run_func)
            return run_func_args

        # Extra arg
        run_func_args = _get_args(BAD_RUN_EXTRA_ARG)
        self.assertEqual(sorted(['self', 'psm', 'other_arg']), sorted(run_func_args))

        # Missing arg
        run_func_args = _get_args(BAD_RUN_MISSING_ARG)
        self.assertEqual(sorted(['self']), sorted(run_func_args))

        # No psm
        run_func_args = _get_args(BAD_RUN_NO_PSM)
        self.assertEqual(sorted(['self', 'other_arg']), sorted(run_func_args))

        # No run function
        parser = ast_parser.AstParser(NO_RUN_FUNC, MODULE_TEST_CASES)
        prism_task_class = parser.get_prism_task_node(parser.classes, parser.bases)
        run_func = parser.get_run_func(prism_task_class)
        self.assertIsNone(run_func)

        for module in [BAD_RUN_EXTRA_ARG, BAD_RUN_MISSING_ARG, BAD_RUN_NO_PSM, NO_RUN_FUNC]:
            with self.assertRaises(prism.exceptions.ParserException) as cm:
                parser = ast_parser.AstParser(module, MODULE_TEST_CASES)
                parser.parse()
            if module==NO_RUN_FUNC:
                expected_msg = f"no `run` function in PrismTask in `{str(module)}`"
            else:
                msg_list = [
                    f'invalid arguments in `run` function in PrismTask in {str(module)}',
                    f'should only be `self` and `psm`'
                ]
                expected_msg = '\n'.join(msg_list)
            self.assertEqual(expected_msg, str(cm.exception))


# EOF