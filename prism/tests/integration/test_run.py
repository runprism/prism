"""
Integration tests.

Table of Contents:
- Imports
- Test case directory and paths
- Expected events
- Test case class definition
"""

###########
# Imports #
###########

# Standard library imports
from io import StringIO
import pytest
import json
import pandas as pd
import os
from pathlib import Path
import shutil
from typing import Dict, List

# Prism imports
from click.testing import CliRunner
from prism.main import cli
from prism.prism_logging import string_stream_handler
import prism.tests.integration.integration_test_class as integration_test_class


#################################
# Test case directory and paths #
#################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TEST_PROJECTS = Path(TEST_CASE_WKDIR) / 'test_projects'


###################
# Expected events #
###################

def _execution_events_tasks(task_names_statuses: dict) -> list:
    """
    Create list for execution events

    args:
        task_names_statuses: dict mapping event_name --> event_status
    returns:
        list of execution events
    """
    results = []
    for k, v in task_names_statuses.items():
        results.append(f'ExecutionEvent - {k} - RUN')
        results.append(f'ExecutionEvent - {k} - {v}')
    return results


def _run_task_end_events(end_event_name: str) -> list:
    """
    Create list for events marking the end of the run task

    args:
        end_event_name: name of end event
    returns:
        list of events marking end of run task
    """
    return [
        'EmptyLineEvent',
        end_event_name,
        'SeparatorEvent'
    ]


# Starting events for a successful run task. We will only run tasks that successfully
# compile, since we have dealt with compile edge cases in the `test_compile` integration
# tests.
run_success_starting_events = [
    'SeparatorEvent',
    'TaskRunEvent',
    'CurrentProjectDirEvent',
    'EmptyLineEvent',
    'ExecutionEvent - parsing prism_project.py - RUN',
    'ExecutionEvent - parsing prism_project.py - DONE',
    'ExecutionEvent - task DAG - RUN',
    'ExecutionEvent - task DAG - DONE',
    'ExecutionEvent - creating pipeline, DAG executor - RUN',
    'ExecutionEvent - creating pipeline, DAG executor - DONE',
    'EmptyLineEvent'
]


simple_project_all_tasks_expected_events = run_success_starting_events + \
    ['TasksHeaderEvent'] + \
    _execution_events_tasks({'module03.Task03': 'ERROR'}) + \
    _run_task_end_events('PrismExceptionErrorEvent')

simple_project_no_null_all_tasks_expected_events = run_success_starting_events + \
    ['TasksHeaderEvent'] + \
    _execution_events_tasks({
        'module01.Task01': 'DONE',
        'module02.Task02': 'DONE',
        'module03.Task03': 'DONE',
        'module04.Task04': 'DONE',
    }) + _run_task_end_events('TaskSuccessfulEndEvent')


##############################
# Test case class definition #
##############################

class TestRunIntegration(integration_test_class.IntegrationTestCase):

    def test_simple_project_all_tasks(self):
        """
        `prism run` on simple project with a null task output
        """
        self.maxDiff = None

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '004_simple_project'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Execute command
        args = ['run']
        runtask_run = self._run_prism(args)
        runtask_run_results = runtask_run.get_results()
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.json').is_file())

        # Check events
        self.assertEqual(
            ' | '.join(simple_project_all_tasks_expected_events),
            runtask_run_results
        )

        # Check manifest.json
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.json'))
        task01_refs = self._load_task_refs("module01", "Task01", manifest)
        task02_refs = self._load_task_refs("module02", "Task02", manifest)
        task03_refs = self._load_task_refs("module03", "Task03", manifest)
        self.assertEqual([], task01_refs)
        self.assertEqual(['module01.Task01'], task02_refs)
        self.assertEqual([], task03_refs)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_simple_project_no_null_modules_all_tasks(self):
        """
        `prism run` on simple project with no null task outputs
        """
        self.maxDiff = None

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '005_simple_project_no_null_modules'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Execute command
        args = ['run']
        runtask_run = self._run_prism(args)
        runtask_run_results = runtask_run.get_results()
        self.assertEqual(
            ' | '.join(simple_project_no_null_all_tasks_expected_events),
            runtask_run_results
        )
        self.assertTrue(Path(wkdir / 'output' / 'task01.txt').is_file())
        self.assertTrue(Path(wkdir / 'output' / 'task02.txt').is_file())

        # Check contents
        task01_txt = self._file_as_str(Path(wkdir / 'output' / 'task01.txt'))
        task02_txt = self._file_as_str(Path(wkdir / 'output' / 'task02.txt'))
        self.assertEqual('Hello from task 1!', task01_txt)
        self.assertEqual(
            'Hello from task 1!' + '\n' + 'Hello from task 2!',
            task02_txt
        )

        # Check that certain messages are logged
        msg1 = "WARN  | `modules` should be renamed to `tasks`...this will be an error in a future version of Prism"  # noqa: E501
        msg2 = "WARN  | Found `.py` in a tasks.ref(...) argument in `module02.py`...This will be an error in a future version of Prism."  # noqa: E501
        msg3 = "WARN  | Found `.py` in a tasks.ref(...) argument in `module03.py`...This will be an error in a future version of Prism."  # noqa: E501
        msg4 = "WARN  | Found `.py` in a tasks.ref(...) argument in `module04.py`...This will be an error in a future version of Prism."  # noqa: E501
        logs = new_streamer.getvalue()
        for msg in [msg1, msg2, msg3, msg4]:
            self.assertTrue(msg in logs)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_simple_project_no_null_tasks_all_tasks(self):
        """
        `prism run` on simple project with no null task outputs
        """
        self.maxDiff = None

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '005_simple_project_no_null_tasks'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Execute command
        args = ['run']
        runtask_run = self._run_prism(args)
        runtask_run_results = runtask_run.get_results()
        self.assertEqual(
            ' | '.join(simple_project_no_null_all_tasks_expected_events),
            runtask_run_results
        )
        self.assertTrue(Path(wkdir / 'output' / 'task01.txt').is_file())
        self.assertTrue(Path(wkdir / 'output' / 'task02.txt').is_file())

        # Check contents
        task01_txt = self._file_as_str(Path(wkdir / 'output' / 'task01.txt'))
        task02_txt = self._file_as_str(Path(wkdir / 'output' / 'task02.txt'))
        self.assertEqual('Hello from task 1!', task01_txt)
        self.assertEqual(
            'Hello from task 1!' + '\n' + 'Hello from task 2!',
            task02_txt
        )

        # Check logs should't contain any warning
        msg1 = "WARN  | `modules` should be renamed to `tasks`...this will be an error in a future version of Prism"  # noqa: E501
        msg2 = "WARN  | Found `.py` in a tasks.ref(...) argument in `module02.py`...This will be an error in a future version of Prism."  # noqa: E501
        msg3 = "WARN  | Found `.py` in a tasks.ref(...) argument in `module03.py`...This will be an error in a future version of Prism."  # noqa: E501
        msg4 = "WARN  | Found `.py` in a tasks.ref(...) argument in `module04.py`...This will be an error in a future version of Prism."  # noqa: E501
        logs = new_streamer.getvalue()
        for msg in [msg1, msg2, msg3, msg4]:
            self.assertFalse(msg in logs)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_simple_project_no_null_subset(self):
        """
        `prism run` on simple project with no null task outputs
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '005_simple_project_no_null_tasks'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # *************** #
        # Run only task 1 #
        # *************** #

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Expecatation: task 1 is the first task in the DAG. Therefore, we should
        # not encounter any errors with this command.
        args = ['run', '--task', 'module01.py']
        runtask_run = self._run_prism(args)
        runtask_run_results = runtask_run.get_results()
        expected_events = run_success_starting_events + \
            ['TasksHeaderEvent'] + \
            _execution_events_tasks({'module01.Task01': 'DONE'}) + \
            _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), runtask_run_results)

        # Check the results of the output directory
        self.assertTrue(Path(wkdir / 'output' / 'task01.txt').is_file())
        self.assertFalse(Path(wkdir / 'output' / 'task02.txt').is_file())
        task01_txt = self._file_as_str(Path(wkdir / 'output' / 'task01.txt'))
        self.assertEqual('Hello from task 1!', task01_txt)

        # Check manifest
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.json').is_file())
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.json'))
        task01_refs = self._load_task_refs("module01", "Task01", manifest)
        task02_refs = self._load_task_refs("module02", "Task02", manifest)
        task03_refs = self._load_task_refs("module03", "Task03", manifest)
        task04_refs = self._load_task_refs("module04", "Task04", manifest)
        self.assertEqual([], task01_refs)
        self.assertEqual(['module01.Task01'], task02_refs)
        self.assertEqual(['module02.Task02'], task03_refs)
        self.assertEqual(['module03.Task03'], task04_refs)

        # Logs should have a warning about `.py` in task argument
        runner = CliRunner()
        result = runner.invoke(cli, args)
        expected_msg = "ArgumentWarning: `.py` in --task arguments will be an error in a future version of Prism.\n"  # noqa: E501
        self.assertEqual(expected_msg, result.output)
        self.assertEqual(0, result.exit_code)

        # ************** #
        # Execute task 2 #
        # ************** #

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Expecatation: task 2 depends on task 1. However, since we just ran task
        # 1, and the output of task 1 is stored in a target, we do not need to re-run
        # task 1 in order to run task 2. Therefore, we should not encounter any
        # errors with this command.

        # Execute command
        args = ['run', '--task', 'module02', '--full-tb']
        runtask_run = self._run_prism(args)
        runtask_run_results = runtask_run.get_results()
        expected_events = run_success_starting_events + \
            ['TasksHeaderEvent'] + \
            _execution_events_tasks({'module02.Task02': 'DONE'}) + \
            _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), runtask_run_results)

        # Check the results of the output directory
        self.assertTrue(Path(wkdir / 'output' / 'task02.txt').is_file())
        with open(Path(wkdir / 'output' / 'task02.txt'), 'r') as f:
            task02_txt = f.read()
        f.close()
        self.assertEqual(
            'Hello from task 1!' + '\n' + 'Hello from task 2!', task02_txt
        )

        # ************************************************* #
        # Execute task 4 (with and without `all-upstream` #
        # ************************************************* #

        # Expectation: task 4 depends on task 3. However, the output of task 3 is
        # not stored in a target. Therefore, running task 4 without including
        # 'all-upstream' should cause an error.

        # -------------------------------------
        # Execute command without `all-upstream`

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        args = ['run', '--task', 'module04']
        runtask_run = self._run_prism(args)
        runtask_run_results = runtask_run.get_results()
        expected_events = run_success_starting_events + \
            ['TasksHeaderEvent'] + \
            _execution_events_tasks({'module04.Task04': 'ERROR'}) + \
            _run_task_end_events('PrismExceptionErrorEvent')
        self.assertEqual(' | '.join(expected_events), runtask_run_results)

        # -----------------------------------
        # Execute command with `all-upstream`

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        self._remove_compiled_dir(wkdir)
        self._remove_files_in_output(wkdir)
        args = ['run', '--task', 'module04', '--all-upstream']
        runtask_run = self._run_prism(args)
        runtask_run_results = runtask_run.get_results()
        self.assertEqual(
            ' | '.join(simple_project_no_null_all_tasks_expected_events),
            runtask_run_results
        )

        # Check the results of the output directory
        self.assertTrue(Path(wkdir / 'output' / 'task01.txt').is_file())
        self.assertTrue(Path(wkdir / 'output' / 'task02.txt').is_file())
        task02_txt = self._file_as_str(Path(wkdir / 'output' / 'task02.txt'))
        self.assertEqual(
            'Hello from task 1!' + '\n' + 'Hello from task 2!', task02_txt
        )

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_project_nested_task_dirs(self):
        """
        `prism run` in a project with directories in the tasks folder
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '010_project_nested_module_dirs'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Helper function
        def check_tasks_1_2_results():
            """
            Helper function to check the results of running tasks 1 and 2. We will use
            this a couple of times.
            """
            # Check that .compiled directory is formed
            self.assertTrue(Path(wkdir / '.compiled').is_dir())
            self.assertTrue(Path(wkdir / '.compiled' / 'manifest.json').is_file())

            # Check that outputs are created
            self.assertTrue(Path(wkdir / 'output' / 'task01.txt').is_file())
            self.assertTrue(Path(wkdir / 'output' / 'task02.txt').is_file())
            with open(Path(wkdir / 'output' / 'task02.txt'), 'r') as f:
                task02_txt = f.read()
            f.close()
            self.assertEqual(
                'Hello from task 1!' + '\n' + 'Hello from task 2!',
                task02_txt
            )

        # **************************************************** #
        # Execute all tasks in extract folder using '*' syntax #
        # **************************************************** #

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Run Prism
        args = ['run', '--task', 'extract/*']
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = run_success_starting_events + \
            ['TasksHeaderEvent'] + \
            _execution_events_tasks(
                {
                    'extract/module01.Task01': 'DONE',
                    'extract/module02.Task02': 'DONE'
                }) + \
            _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), run_results)

        # Check manifest
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.json'))
        extract_task01_refs = self._load_task_refs(
            "extract/module01",
            "Task01",
            manifest
        )
        extract_task02_refs = self._load_task_refs(
            "extract/module02",
            "Task02",
            manifest
        )
        load_task03_refs = self._load_task_refs(
            "load/module03",
            "Task03",
            manifest
        )
        task04_refs = self._load_task_refs(
            "module04",
            "Task04",
            manifest
        )
        self.assertEqual(
            [], extract_task01_refs
        )
        self.assertEqual(
            ["extract/module01.Task01"], extract_task02_refs
        )
        self.assertEqual(
            ["extract/module02.Task02"], load_task03_refs
        )
        self.assertEqual(
            ["load/module03.Task03"], task04_refs
        )

        # Check results
        check_tasks_1_2_results()

        # Remove all files in the compiled and output directory
        self._remove_compiled_dir(wkdir)
        self._remove_files_in_output(wkdir)

        # *************************************************************** #
        # Execute all tasks in extract /load folder using explicit syntax #
        # *************************************************************** #

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        args = [
            'run',
            '--task',
            'extract/module01.Task01',
            '--task',
            'extract/module02',
            '--task',
            'load/module03.Task03'
        ]
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = run_success_starting_events + \
            ['TasksHeaderEvent'] + \
            _execution_events_tasks(
                {
                    'extract/module01.Task01': 'DONE',
                    'extract/module02.Task02': 'DONE',
                    'load/module03.Task03': 'DONE',
                }) + \
            _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), run_results)

        # Check results
        check_tasks_1_2_results()

        # Remove all files in the compiled and output directory
        self._remove_compiled_dir(wkdir)
        self._remove_files_in_output(wkdir)

        # ***************** #
        # Execute all tasks #
        # ***************** #

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        args = ['run']
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = run_success_starting_events + \
            ['TasksHeaderEvent'] + \
            _execution_events_tasks(
                {
                    'extract/module01.Task01': 'DONE',
                    'extract/module02.Task02': 'DONE',
                    'load/module03.Task03': 'DONE',
                    'module04.Task04': 'DONE'
                }) + \
            _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), run_results)

        # Check output of tasks 1 and 2
        check_tasks_1_2_results()

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_bad_task_ref(self):
        """
        `prism run` fails in a project with a bad mod ref
        """
        self.maxDiff = None

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '011_bad_task_ref'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        if Path(wkdir / '.compiled').is_dir():
            shutil.rmtree(Path(wkdir / '.compiled'))

        args = ['run']
        run_run = self._run_prism(args)
        run_run_results = run_run.get_results()
        expected_events = [
            'SeparatorEvent',
            'TaskRunEvent',
            'CurrentProjectDirEvent',
            'EmptyLineEvent',
            'ExecutionEvent - parsing prism_project.py - RUN',
            'ExecutionEvent - parsing prism_project.py - DONE',
            'ExecutionEvent - task DAG - RUN',
            'ExecutionEvent - task DAG - ERROR',
        ] + _run_task_end_events('PrismExceptionErrorEvent')
        self.assertEqual(' | '.join(expected_events), run_run_results)

        # The .compiled directory will be created
        self.assertTrue(Path(wkdir / '.compiled').is_dir())

        # But, the manifest file will not be
        self.assertFalse(Path(wkdir / '.compiled' / 'manifest.json').is_file())

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_concurrency(self):
        """
        Test concurrent behavior when threads>1
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '012_concurrency'
        os.chdir(wkdir)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Remove the .compiled directory, if it exists
        if Path(wkdir / '.compiled').is_dir():
            shutil.rmtree(Path(wkdir / '.compiled'))
        self.maxDiff = None
        args = ['run']
        self._run_prism(args)

        # Get times
        task2_times = pd.read_csv(wkdir / 'output' / 'task02.csv')
        task1_times = pd.read_csv(wkdir / 'output' / 'task01.csv')

        # Task 1 and 2 should start at the same time
        task2_start_time = int(task2_times['start_time'][0])
        task1_start_time = int(task1_times['start_time'][0])
        self.assertTrue(abs(task2_start_time - task1_start_time) <= 1)

        # Task 2 should finish before task 1
        task2_end_time = int(task2_times['end_time'][0])
        task1_end_time = int(task1_times['end_time'][0])
        self.assertTrue(task2_end_time < task1_end_time)
        self.assertTrue(abs(10 - (task1_end_time - task2_end_time)) <= 1)

        msg1 = "WARN  | `modules` should be renamed to `tasks`...this will be an error in a future version of Prism"  # noqa: E501
        msg2 = "WARN  | Found `.py` in a tasks.ref(...) argument in `module04.py`...This will be an error in a future version of Prism."  # noqa: E501
        msg3 = "WARN  | Found `.py` in a tasks.ref(...) argument in `module03.py`...This will be an error in a future version of Prism."  # noqa: E501
        logs = new_streamer.getvalue()
        for msg in [msg1, msg2, msg3]:
            self.assertTrue(msg in logs)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove stuff in output to avoid recommitting to github
        self._remove_files_in_output(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_concurrency_local(self):
        """
        Test concurrent behavior when threads>1
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '012_concurrency_local'
        os.chdir(wkdir)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Remove stuff in output to avoid recommitting to github
        self._remove_files_in_output(wkdir)

        # Remove the .compiled directory, if it exists
        if Path(wkdir / '.compiled').is_dir():
            shutil.rmtree(Path(wkdir / '.compiled'))
        self.maxDiff = None

        # Run Prism
        args = [
            'run',
            "--task", "local_tasks.local_task3",
            "--all-upstream"
        ]
        self._run_prism(args)

        # Confirm appropriate outputs were created
        for path in [
            Path(wkdir / 'output' / 'local_task2.csv'),
            Path(wkdir / 'output' / 'local_task1.csv')
        ]:
            self.assertTrue(path.is_file())
        self.assertFalse(Path(wkdir / 'output' / 'non_local_task.txt').is_file())

        # Check times
        task2_times = pd.read_csv(wkdir / 'output' / 'local_task2.csv')
        task1_times = pd.read_csv(wkdir / 'output' / 'local_task1.csv')

        # Task 1 and 2 should start at the same time
        task2_start_time = int(task2_times['start_time'][0])
        task1_start_time = int(task1_times['start_time'][0])
        self.assertTrue(abs(task2_start_time - task1_start_time) <= 1)

        # Task 2 should finish before task 1
        task2_end_time = int(task2_times['end_time'][0])
        task1_end_time = int(task1_times['end_time'][0])
        self.assertTrue(task2_end_time < task1_end_time)
        self.assertTrue(abs(10 - (task1_end_time - task2_end_time)) <= 1)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove stuff in output to avoid recommitting to github
        self._remove_files_in_output(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_user_context_cli(self):
        """
        Test that CLI user context works as expected
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '005_simple_project_no_null_tasks'
        os.chdir(wkdir)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Remove the .compiled directory, if it exists
        if Path(wkdir / '.compiled').is_dir():
            shutil.rmtree(Path(wkdir / '.compiled'))
        self.maxDiff = None

        # Remove files in output folder
        self._remove_files_in_output(wkdir)

        # New output path
        output_path = str(wkdir.parent)
        self.assertFalse((Path(output_path) / 'task01.txt').is_file())
        args = ['run', '--task', 'module01', '--vars', f'OUTPUT={output_path}']
        self._run_prism(args)

        # Get output
        self.assertTrue((Path(output_path) / 'task01.txt').is_file())
        task01_txt = self._file_as_str(Path(output_path) / 'task01.txt')
        self.assertEqual('Hello from task 1!', task01_txt)
        os.unlink(Path(output_path) / 'task01.txt')

        # Re-run to place output in normal directory
        args = ['run']
        self._run_prism(args)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_all_downstream(self):
        """
        Test that `all-downstream` argument functions as expected
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '005_simple_project_no_null_tasks'
        os.chdir(wkdir)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Run all tasks downstream of module01.py
        args = ['run', '--task', 'module01', '--all-downstream']
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = run_success_starting_events + \
            ['TasksHeaderEvent'] + \
            _execution_events_tasks(
                {
                    'module01.Task01': 'DONE',
                    'module02.Task02': 'DONE',
                    'module03.Task03': 'DONE',
                    'module04.Task04': 'DONE',
                }) + \
            _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), run_results)

        # Check manifest
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / '.compiled' / 'manifest.json').is_file())
        manifest = self._load_manifest(Path(wkdir / '.compiled' / 'manifest.json'))
        task01_refs = self._load_task_refs("module01", "Task01", manifest)
        task02_refs = self._load_task_refs("module02", "Task02", manifest)
        task03_refs = self._load_task_refs("module03", "Task03", manifest)
        task04_refs = self._load_task_refs("module04", "Task04", manifest)
        self.assertEqual([], task01_refs)
        self.assertEqual(["module01.Task01"], task02_refs)
        self.assertEqual(["module02.Task02"], task03_refs)
        self.assertEqual(["module03.Task03"], task04_refs)

        # Check that outputs are created
        self.assertTrue(Path(wkdir / 'output' / 'task01.txt').is_file())
        self.assertTrue(Path(wkdir / 'output' / 'task02.txt').is_file())
        with open(Path(wkdir / 'output' / 'task02.txt'), 'r') as f:
            task02_txt = f.read()
        f.close()
        self.assertEqual(
            'Hello from task 1!' + '\n' + 'Hello from task 2!',
            task02_txt
        )

        # Remove all files in the compiled directory
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def _check_trigger_output(self, wkdir: Path):
        """
        Our test trigger function outputs a .txt file to the wkdir / 'output' folder.
        Check that this exists.
        """
        self.assertTrue(Path(wkdir / '.compiled').is_dir())
        self.assertTrue(Path(wkdir / 'output' / 'trigger.txt').is_file())
        with open(Path(wkdir / 'output' / 'trigger.txt'), 'r') as f:
            trigger_txt = f.read()
        self.assertEqual('This is outputted from the trigger function!', trigger_txt)

    def _check_trigger_events(self,
        execution_event_dict: Dict[str, str],
        circa_trigger_header_event: List[str] = ['TriggersHeaderEvent'],
        final_status: str = "DONE"
    ):
        """
        Triggers kick of a predictable set of events. Check that these exist.
        """
        expected_events = run_success_starting_events + \
            ['TasksHeaderEvent'] + \
            _execution_events_tasks(execution_event_dict) + \
            ["EmptyLineEvent"] + \
            circa_trigger_header_event + \
            [
                "ExecutionEvent - test_trigger_function - RUN",  # noqa: E501
                f"ExecutionEvent - test_trigger_function - {final_status}",  # noqa: E501
            ]
        return expected_events

    def test_trigger_on_success(self):
        """
        Test on_success trigger
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '014_test_triggers_normal'
        os.chdir(wkdir)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Run all tasks downstream of module01.py
        args = ['run', '--task', 'module01']
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = self._check_trigger_events(
            {'module01.Task01': 'DONE'}
        ) + _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), run_results)

        # Check manifest / output
        self._check_trigger_output(wkdir)

        # Remove all files in the compiled directory
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_trigger_on_failure(self):
        """
        Test on_failure trigger
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '014_test_triggers_normal'
        os.chdir(wkdir)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Run all tasks downstream of module01.py
        args = ['run', '--task', 'module02']
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = self._check_trigger_events(
            {'module02.Task02': 'ERROR'},
            ["ExecutionErrorEvent", "TriggersHeaderEvent"]
        ) + ["SeparatorEvent"]
        self.assertEqual(' | '.join(expected_events), run_results)

        # Check manifest / output
        self._check_trigger_output(wkdir)

        # Remove all files in the compiled directory
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_trigger_no_directory(self):
        """
        Test trigger function when there is no `TRIGGERS_YML_PATH` in prism_project.py
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '015_test_triggers_no_dir'
        os.chdir(wkdir)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Run all tasks downstream of module01.py
        args = ['run', '--task', 'module01']
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = self._check_trigger_events(
            {'module01.Task01': 'DONE'},
            ["TriggersHeaderEvent", "TriggersPathNotDefined"]
        ) + _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), run_results)

        # Check that manifest was created
        self._check_trigger_output(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_bad_trigger(self):
        """
        Test trigger function with a bad trigger
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '016_test_triggers_error'
        os.chdir(wkdir)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Run all tasks downstream of module01.py
        args = ['run']
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = self._check_trigger_events(
            {'module01.Task01': 'DONE'},
            ["TriggersHeaderEvent"],
            'ERROR'
        ) + ['EmptyLineEvent', 'ExecutionErrorEvent', 'SeparatorEvent']
        self.assertEqual(' | '.join(expected_events), run_results)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_triggers_with_extra_key(self):
        """
        An extra key in `triggers.yml` should raise a warning
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '017_test_triggers_extra_key'
        os.chdir(wkdir)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Run all tasks downstream of module01.py
        args = ['run']
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = self._check_trigger_events(
            {'module01.Task01': 'DONE'},
            ["TriggersHeaderEvent", "UnexpectedTriggersYmlKeysEvent"]
        ) + _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), run_results)

        # Check that manifest was created
        self._check_trigger_output(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_triggers_no_include(self):
        """
        A trigger function in an external task/package without an accompanying
        `include` path will throw an error.
        """

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '018_test_triggers_no_include'
        os.chdir(wkdir)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Run all tasks downstream of module01.py
        args = ['run']
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = self._check_trigger_events(
            {'module01.Task01': 'DONE'},
            ["TriggersHeaderEvent"],
            'ERROR'
        ) + ['EmptyLineEvent', 'ExecutionErrorEvent', 'SeparatorEvent']
        self.assertEqual(' | '.join(expected_events), run_results)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_decorator_tasks_with_targets(self):
        """
        `prism run` on a project where all the tasks are functions decorated with
        `@task`
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '019_dec_targets'
        os.chdir(wkdir)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Run
        args = ['run']
        runtask_run = self._run_prism(args)
        runtask_run_results = runtask_run.get_results()
        expected_events = self._check_trigger_events(
            {
                'extract.extract': 'DONE',
                'load.load': 'DONE'
            },
        ) + _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), runtask_run_results)

        # Check output of 'extract' task
        self.assertTrue(Path(wkdir / 'output' / 'astros.json').is_file())
        self.assertTrue(Path(wkdir / 'output' / 'second_target.txt').is_file())

        # Astros JSON file
        with open(Path(wkdir / 'output' / 'astros.json'), 'r') as f:
            astros_str = f.read()
        astros_dict = json.loads(astros_str)
        self.assertEqual(astros_dict["message"], "success")

        # Dummy second target text file
        with open(Path(wkdir / 'output' / 'second_target.txt'), 'r') as f:
            second_target = f.read()
        self.assertEqual(second_target, "second target")

        # Check output of 'load' task
        names = [
            "Sergey Prokopyev",
            "Dmitry Petelin",
            "Frank Rubio",
            "Stephen Bowen",
            "Warren Hoburg",
            "Sultan Alneyadi",
            "Andrey Fedyaev",
            "Jing Haiping",
            "Gui Haichow",
            "Zhu Yangzhu",
        ]
        for n in names:
            formatted_name = n.lower().replace(" ", "_")
            self.assertTrue(Path(wkdir / 'output' / f'{formatted_name}.txt').is_file())
            with open(Path(wkdir / 'output' / f'{formatted_name}.txt'), 'r') as f:
                contents = f.read()
            self.assertEqual(contents, n)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up the working directory
        self._set_up_wkdir()

    def test_decorator_tasks_with_retries(self):
        """
        `prism run` on a project where all the tasks are functions decorated with
        `@task` and the user wants to retry a task
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '020_dec_retries'
        os.chdir(wkdir)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Run
        args = ['run']
        runtask_run = self._run_prism(args)
        runtask_run_results = runtask_run.get_results()
        expected_events = run_success_starting_events + \
            ['TasksHeaderEvent'] + \
            _execution_events_tasks(
                {
                    'extract.extract': 'DONE',
                    'load.load': 'ERROR'
                },
            ) + \
            ['DelayEvent'] + \
            _execution_events_tasks(
                {'load.load (RETRY 1)': 'ERROR'}
            ) + \
            [
                "EmptyLineEvent",
                "ExecutionErrorEvent",
                "TriggersHeaderEvent",
                "ExecutionEvent - test_trigger_function - RUN",
                "ExecutionEvent - test_trigger_function - DONE",
                "SeparatorEvent"
            ]
        self.assertEqual(' | '.join(expected_events), runtask_run_results)

        # Check output of 'extract' task
        self.assertTrue(Path(wkdir / 'output' / 'astros.json').is_file())
        with open(Path(wkdir / 'output' / 'astros.json'), 'r') as f:
            astros_str = f.read()
        astros_dict = json.loads(astros_str)
        self.assertEqual(astros_dict["message"], "success")

        # Output of 'load' task was not created
        names = [
            "Andrey Fedyaev",
            "Deng Qingming",
            "Dmitry Petelin",
            "Fei Junlong",
            "Frank Rubio",
            "Sergey Prokopyev",
            "Stephen Bowen",
            "Sultan Alneyadi",
            "Warren Hoburg",
            "Zhang Lu",
        ]
        for n in names:
            formatted_name = n.lower().replace(" ", "_")
            self.assertFalse(Path(wkdir / 'output' / f'{formatted_name}.txt').is_file())

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Set up the working directory
        self._set_up_wkdir()

    def test_simple_project_tasks_prefix_in_arg(self):
        """
        `prism run` on simple project where `tasks/` is included in the `--task`
        CLI argument
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '005_simple_project_no_null_tasks'
        os.chdir(wkdir)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Execute command. Remove the `.py` from the command as well.
        args = [
            'run',
            '--task', 'tasks/module01',
            '--task', 'tasks/module02',
            '--task', 'tasks/module03',
            '--task', 'tasks/module04',
        ]
        with pytest.raises(SystemExit) as cm:
            self._run_prism(args)
        self.assertEqual(cm.value.code, 1)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_project_local_tasks(self):
        """
        `prism run` on a project with local tasks works as expected
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '021_project_with_local_tasks'
        os.chdir(wkdir)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)
        self.assertFalse(Path(wkdir / 'output' / 'all_countries.json').is_file())
        self.assertFalse(Path(wkdir / 'output' / 'independent_countries.json').is_file())  # noqa: E501

        # Execute command.
        args = ['run']
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = run_success_starting_events + \
            ['TasksHeaderEvent'] + \
            _execution_events_tasks(
                {
                    'extract.extract': 'DONE',
                    'transform_load.transform': 'DONE',
                    'transform_load.load': 'DONE',
                }) + \
            _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), run_results)

        # Check output
        self.assertTrue(Path(wkdir / 'output' / 'all_countries.json').is_file())
        self.assertTrue(Path(wkdir / 'output' / 'independent_countries.json').is_file())
        with open(Path(wkdir / 'output' / 'all_countries.json'), 'r') as f:
            all_countries = json.loads(f.read())
        with open(Path(wkdir / 'output' / 'independent_countries.json'), 'r') as f:
            independent_countries = json.loads(f.read())
        self.assertEqual(all_countries[-1]["name"]["common"], "Guinea-Bissau")
        self.assertEqual(independent_countries[0]["name"]["common"], "Jordan")

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_project_local_tasks_specific_module(self):
        """
        Using the `--task` argument on a prism project with local tasks will run all the
        tasks in the requested module.
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '021_project_with_local_tasks'
        os.chdir(wkdir)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # ------------------------------------------
        # Execute command. First, run just `extract`

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        self.assertFalse(Path(wkdir / 'output' / 'all_countries.json').is_file())  # noqa: E501
        args = [
            'run',
            '--task', 'extract'
        ]
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = run_success_starting_events + \
            ['TasksHeaderEvent'] + \
            _execution_events_tasks(
                {
                    'extract.extract': 'DONE',
                }) + \
            _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), run_results)

        # Check output
        self.assertTrue(Path(wkdir / 'output' / 'all_countries.json').is_file())
        self.assertFalse(
            Path(wkdir / 'output' / 'independent_countries.json').is_file()
        )
        with open(Path(wkdir / 'output' / 'all_countries.json'), 'r') as f:
            all_countries = json.loads(f.read())
        self.assertEqual(all_countries[-1]["name"]["common"], "Guinea-Bissau")

        # ------------------------------------------
        # Now, execute transform_load

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        args = [
            'run',
            '--task', 'transform_load'
        ]
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = run_success_starting_events + \
            ['TasksHeaderEvent'] + \
            _execution_events_tasks(
                {
                    'transform_load.transform': 'DONE',
                    'transform_load.load': 'DONE',
                }) + \
            _run_task_end_events('TaskSuccessfulEndEvent')
        self.assertEqual(' | '.join(expected_events), run_results)

        # Check output
        self.assertTrue(Path(wkdir / 'output' / 'independent_countries.json').is_file())
        with open(Path(wkdir / 'output' / 'independent_countries.json'), 'r') as f:
            independent_countries = json.loads(f.read())
        self.assertEqual(independent_countries[0]["name"]["common"], "Jordan")

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()

    def test_project_local_tasks_bad_ref(self):
        """
        A bad `tasks.ref(...)` call to a local task will cause an error.
        """
        self.maxDiff = None

        # Set working directory
        wkdir = Path(TEST_PROJECTS) / '022_project_with_bad_local_tasks'
        os.chdir(wkdir)

        # Update logger streamer
        new_streamer = StringIO()
        string_stream_handler.setStream(new_streamer)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # ------------------------------------------
        # Execute command.
        self.assertFalse(Path(wkdir / 'output' / 'all.json').is_file())  # noqa: E501
        self.assertFalse(Path(wkdir / 'output' / 'independent_countries.json').is_file())  # noqa: E501
        args = ['run']
        run = self._run_prism(args)
        run_results = run.get_results()
        expected_events = [
            'SeparatorEvent',
            'TaskRunEvent',
            'CurrentProjectDirEvent',
            'EmptyLineEvent',
            'ExecutionEvent - parsing prism_project.py - RUN',
            'ExecutionEvent - parsing prism_project.py - DONE',
            'ExecutionEvent - task DAG - RUN',
            'ExecutionEvent - task DAG - ERROR',
            'EmptyLineEvent',
            'PrismExceptionErrorEvent',
            'SeparatorEvent'
        ]
        self.assertEqual(' | '.join(expected_events), run_results)

        # Check output
        self.assertFalse(Path(wkdir / 'output' / 'all.json').is_file())  # noqa: E501
        self.assertFalse(Path(wkdir / 'output' / 'independent_countries.json').is_file())  # noqa: E501

        # Error
        logs = new_streamer.getvalue()
        msg = "Are you trying to access a task in the same module? If so, use only the task name as your tasks.ref() argument and set `local = True`"  # noqa: E501
        self.assertTrue(msg in logs)

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(wkdir)

        # Remove all files in the output directory
        self._remove_files_in_output(wkdir)

        # Set up wkdir for the next test case
        self._set_up_wkdir()
