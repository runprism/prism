"""
Integration tests.

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
import shutil
import pandas as pd

# Prism imports
import prism.client
import prism.tests.integration.test_connect as test_connect
import prism.tests.integration.integration_test_class as integration_test_class
import prism.exceptions


#################################
# Test case directory and paths #
#################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TEST_PROJECTS = Path(TEST_CASE_WKDIR) / 'test_projects'

# Project directories
P002_NO_PROJECT_PY = Path(TEST_PROJECTS / '002_no_project_py')
P003_PROJECT_WITH_CYCLE = Path(TEST_PROJECTS / '003_project_with_cycle')
P004_SIMPLE_PROJECT = Path(TEST_PROJECTS / '004_simple_project')
P005_SIMPLE_PROJECT_NO_NULL = Path(TEST_PROJECTS / '005_simple_project_no_null_tasks')
P006_SIMPLE_PROJECT_WITH_PROFILE = Path(TEST_PROJECTS / '006_simple_project_with_profile')  # noqa: E501
P007_SPARK_PROJECT = Path(TEST_PROJECTS / '007_spark_project')
P009_SIMPLE_DBT_PROJECT = Path(TEST_PROJECTS / '009_simple_dbt_project' / 'prism')
P014_TEST_TRIGGERS_NORMAL = Path(TEST_PROJECTS / '014_test_triggers_normal')


#############
# Constants #
#############

expected_snowflake_dict = test_connect.expected_snowflake_dict
expected_snowflake_pyspark_dict = test_connect.expected_snowflake_pyspark_dict


##############################
# Test case class definition #
##############################

class TestClient(
    integration_test_class.IntegrationTestCase
):

    def test_prism_dag_init(self):
        """
        Test initialization of PrismDAG object.
        """
        # Invalid project
        with self.assertRaises(prism.exceptions.ProjectPyNotFoundException) as cm:
            prism.client.PrismDAG(P002_NO_PROJECT_PY)
        expected_msg = 'prism_project.py file not found in current directory or any of its parents'  # noqa: E501
        self.assertEqual(expected_msg, str(cm.exception))

        # Valid projects
        for proj in [
            P004_SIMPLE_PROJECT,
            P005_SIMPLE_PROJECT_NO_NULL,
            P006_SIMPLE_PROJECT_WITH_PROFILE,
            P007_SPARK_PROJECT
        ]:
            prism.client.PrismDAG(proj)

    def test_prism_dag_compile_cycle(self):
        """
        PrismDAG compile function in a project with a cycle
        """
        dag = prism.client.PrismDAG(P003_PROJECT_WITH_CYCLE)

        # Remove .compiled dir in project, if it exists
        if Path(P003_PROJECT_WITH_CYCLE / '.compiled').is_dir():
            shutil.rmtree(Path(P003_PROJECT_WITH_CYCLE / '.compiled'))

        # Compile
        with self.assertRaises(prism.exceptions.DAGException) as cm:
            dag.compile()
        expected_msg = "invalid DAG, cycle found in"
        self.assertTrue(expected_msg in str(cm.exception))
        self.assertTrue('module02.Task02' in str(cm.exception))
        self.assertTrue('module03.Task03' in str(cm.exception))

        # Check that manifest is not formed
        self.assertFalse(
            Path(P003_PROJECT_WITH_CYCLE / '.compiled' / 'manifest.json').is_file()
        )

        # Set up directory for next test
        self._set_up_wkdir()

    def test_prism_dag_compile(self):
        """
        PrismDAG compile function
        """
        dag = prism.client.PrismDAG(P004_SIMPLE_PROJECT)

        # Remove .compiled dir in project, if it exists
        if Path(P004_SIMPLE_PROJECT / '.compiled').is_dir():
            shutil.rmtree(Path(P004_SIMPLE_PROJECT / '.compiled'))

        # Compile
        compiled_dag = dag.compile()

        # Check that .compiled directory is formed
        self.assertTrue(Path(P004_SIMPLE_PROJECT / '.compiled').is_dir())
        self.assertTrue(
            Path(P004_SIMPLE_PROJECT / '.compiled' / 'manifest.json').is_file()
        )

        # Check manifest
        manifest = self._load_manifest(
            Path(P004_SIMPLE_PROJECT / '.compiled' / 'manifest.json')
        )
        task01_refs = self._load_task_refs("module01", "Task01", manifest)
        task02_refs = self._load_task_refs("module02", "Task02", manifest)
        task03_refs = self._load_task_refs("module03", "Task03", manifest)
        self.assertEqual([], task01_refs)
        self.assertEqual(['module01.Task01'], task02_refs)
        self.assertEqual([], task03_refs)

        # Check topological sort
        topsort = compiled_dag.topological_sort
        topsort_str = [str(t) for t in topsort]
        self.assertEqual(
            ['module03.Task03', 'module01.Task01', 'module02.Task02'],
            topsort_str
        )

        # Remove the .compiled directory, if it exists
        self._remove_compiled_dir(P004_SIMPLE_PROJECT)

        # Set up directory for next test
        self._set_up_wkdir()

    def test_prism_dag_connect(self):
        """
        PrismDAG connect function
        """
        dag = prism.client.PrismDAG(P006_SIMPLE_PROJECT_WITH_PROFILE)

        # Remove profile YML
        self._remove_profile_yml(P006_SIMPLE_PROJECT_WITH_PROFILE)

        # Connect to snowflake
        dag.connect(connection_type='snowflake')
        profile_yml = self._profile_yml_as_dict(P006_SIMPLE_PROJECT_WITH_PROFILE)
        self.assertEqual(expected_snowflake_dict, profile_yml)

        # Try connecting to Snowflake again, this should produce an error
        with self.assertRaises(prism.exceptions.InvalidProfileException) as cm:
            dag.connect(connection_type='snowflake')
        expected_msg = "adapter with name `snowflake_adapter_name_here` already exists! Change this adapter name and try again"  # noqa: E501
        self.assertEqual(expected_msg, str(cm.exception))

        # Connect to PySpark
        dag.connect(connection_type='pyspark')
        profile_yml = self._profile_yml_as_dict(P006_SIMPLE_PROJECT_WITH_PROFILE)
        self.assertEqual(expected_snowflake_pyspark_dict, profile_yml)

        # Remove and reconnect to Snowflake (to avoid recomitting to Git)
        self._remove_profile_yml(P006_SIMPLE_PROJECT_WITH_PROFILE)
        dag.connect(connection_type='snowflake')

        # Set up directory for next test
        self._set_up_wkdir()

    def test_prism_dag_run(self):
        """
        PrismDAG run function
        """
        dag4 = prism.client.PrismDAG(P004_SIMPLE_PROJECT)
        dag5 = prism.client.PrismDAG(P005_SIMPLE_PROJECT_NO_NULL)
        dag9 = prism.client.PrismDAG(P009_SIMPLE_DBT_PROJECT)

        # ------------------------------------------------------------------------------
        # Try running P004_SIMPLE_PROJECT. It should produce an error because it has a
        # Null output.

        self._remove_compiled_dir(P004_SIMPLE_PROJECT)
        with self.assertRaises(prism.exceptions.RuntimeException) as cm:
            dag4.run()
        expected_msg = '`run` method must produce a non-null output'
        self.assertEqual(expected_msg, str(cm.exception))

        # Confirm creation of manifest
        self.assertTrue(Path(P004_SIMPLE_PROJECT / '.compiled').is_dir())
        self.assertTrue(
            Path(P004_SIMPLE_PROJECT / '.compiled' / 'manifest.json').is_file()
        )

        # Check manifest.json
        manifest = self._load_manifest(
            Path(P004_SIMPLE_PROJECT / '.compiled' / 'manifest.json')
        )
        task01_refs = self._load_task_refs("module01", "Task01", manifest)
        task02_refs = self._load_task_refs("module02", "Task02", manifest)
        task03_refs = self._load_task_refs("module03", "Task03", manifest)
        self.assertEqual([], task01_refs)
        self.assertEqual(['module01.Task01'], task02_refs)
        self.assertEqual([], task03_refs)

        # Cleanup
        self._remove_compiled_dir(P004_SIMPLE_PROJECT)

        # ------------------------------------------------------------------------------
        # Run P005_SIMPLE_PROJECT_NO_NULL (with and without the `tasks` param defined)

        # Remove compiled directory and outputs, if they exist
        self._remove_compiled_dir(P005_SIMPLE_PROJECT_NO_NULL)
        self._remove_files_in_output(P005_SIMPLE_PROJECT_NO_NULL)

        # -------------------------------------------------------
        # With `task` param
        dag5.run(tasks=['module01'])

        # Confirm creation of manifest
        self.assertTrue(Path(P005_SIMPLE_PROJECT_NO_NULL / '.compiled').is_dir())
        self.assertTrue(
            Path(P005_SIMPLE_PROJECT_NO_NULL / '.compiled' / 'manifest.json').is_file()
        )

        # Confirm creation of outputs
        self.assertTrue(
            Path(P005_SIMPLE_PROJECT_NO_NULL / 'output' / 'task01.txt').is_file()
        )
        self.assertFalse(
            Path(P005_SIMPLE_PROJECT_NO_NULL / 'output' / 'task02.txt').is_file()
        )

        # Confirm contents of outputs
        task01_txt = self._file_as_str(
            Path(P005_SIMPLE_PROJECT_NO_NULL / 'output' / 'task01.txt')
        )
        expected_output = 'Hello from task 1!'
        self.assertEqual(expected_output, task01_txt)

        # -------------------------------------------------------
        # Without `task` param
        dag5.run()

        # Confirm creation of outputs
        self.assertTrue(
            Path(P005_SIMPLE_PROJECT_NO_NULL / 'output' / 'task01.txt').is_file()
        )
        self.assertTrue(
            Path(P005_SIMPLE_PROJECT_NO_NULL / 'output' / 'task02.txt').is_file()
        )

        # Confirm contents of outputs
        task02_txt = self._file_as_str(
            Path(P005_SIMPLE_PROJECT_NO_NULL / 'output' / 'task02.txt')
        )
        expected_output = 'Hello from task 1!\nHello from task 2!'
        self.assertEqual(expected_output, task02_txt)

        # Remove compiled directory and outputs, if they exist
        self._remove_compiled_dir(P005_SIMPLE_PROJECT_NO_NULL)

        # ------------------------------------------------------------------------------
        # Run P009_SIMPLE_DBT_PROJECT to confirm that projects with profiles run as
        # expected

        # Remove compiled directory and outputs, if they exist
        self._remove_compiled_dir(P009_SIMPLE_DBT_PROJECT)
        self._remove_files_in_output(P009_SIMPLE_DBT_PROJECT)

        dag9.run(tasks=['filter_customers.py'])

        self.assertTrue(Path(P009_SIMPLE_DBT_PROJECT / '.compiled').is_dir())
        self.assertTrue(
            Path(P009_SIMPLE_DBT_PROJECT / '.compiled' / 'manifest.json').is_file()
        )

        # Check contents of output
        df = pd.read_csv(
            P009_SIMPLE_DBT_PROJECT / 'output' / 'jaffle_shop_customers.csv'
        )
        expected_columns = [
            'CUSTOMER_ID',
            'FIRST_NAME',
            'LAST_NAME',
            'FIRST_ORDER',
            'MOST_RECENT_ORDER',
            'NUMBER_OF_ORDERS',
            'CUSTOMER_LIFETIME_VALUE'
        ]
        self.assertEqual(expected_columns, list(df.columns))
        id_1_first_name = df.loc[df['CUSTOMER_ID'] == 1, 'FIRST_NAME'][0]
        self.assertEqual('Michael', id_1_first_name)

        # Remove the 'target' -- it contains dbt artifacts
        if Path(P009_SIMPLE_DBT_PROJECT / 'target').is_dir():
            shutil.rmtree(Path(P009_SIMPLE_DBT_PROJECT / 'target'))

        # Remove compiled folder
        self._remove_compiled_dir(P009_SIMPLE_DBT_PROJECT)

        # Set up directory for next test
        self._set_up_wkdir()

    def test_run_with_triggers(self):
        """
        Test run with some triggers
        """
        dag = prism.client.PrismDAG(P014_TEST_TRIGGERS_NORMAL)

        # Confirm expected output doesn't exist
        trigger_output = Path(P014_TEST_TRIGGERS_NORMAL / 'output' / 'trigger.txt')
        if trigger_output.is_file():
            os.unlink(trigger_output)
        self.assertFalse(trigger_output.is_file())

        # Run. This will produce an error, but we want to see if our trigger output was
        # created
        try:
            dag.run()
        except Exception:
            pass

        # We expect the trigger to produce an output
        self.assertTrue(trigger_output.is_file())
        with open(trigger_output, 'r') as f:
            trigger_output_str = f.read()
        expected_str = 'This is outputted from the trigger function!'
        self.assertTrue(expected_str, trigger_output_str)

        # Remove compiled folder
        self._remove_compiled_dir(P014_TEST_TRIGGERS_NORMAL)

        # Set up directory for next test
        self._set_up_wkdir()

    def test_get_task_output(self):
        """
        Test task output retrieval
        """
        # Use P005_SIMPLE_PROJECT_NO_NULL for testing
        dag5 = prism.client.PrismDAG(P005_SIMPLE_PROJECT_NO_NULL)

        # Get output of a task without a target (without running pipeline). This should
        # result in an error.
        with self.assertRaises(prism.exceptions.RuntimeException) as cm:
            dag5.get_task_output('module03')
        expected_msg_components = ['cannot access the output of', 'either explicitly running task or setting a target']  # noqa: E501
        for comp in expected_msg_components:
            self.assertTrue(comp in str(cm.exception))

        # Get output of a task with a target (without running pipeline)
        task01_output = dag5.get_task_output('module01.py')
        expected_output = 'Hello from task 1!'
        self.assertEqual(str(task01_output), expected_output)

        # Get output of a task without a target (after running pipeline)
        dag5.run()
        output = dag5.get_task_output('module03.py')
        expected_output = 'Hello from task 1!\nHello from task 2!\nHello from task 3!'  # noqa: E501
        self.assertEqual(expected_output, output)

        # Remove compiled directory and outputs, if they exist
        self._remove_compiled_dir(P005_SIMPLE_PROJECT_NO_NULL)

        # Set up directory for next test
        self._set_up_wkdir()

    def test_get_pipeline_output(self):
        """
        Test pipeline output retrieval
        """
        # Use P005_SIMPLE_PROJECT_NO_NULL for testing
        dag5 = prism.client.PrismDAG(P005_SIMPLE_PROJECT_NO_NULL)

        # Get output of a pipeline without running the pipeline. Since the last task
        # does not have a target, this should result in an error.
        with self.assertRaises(prism.exceptions.RuntimeException) as cm:
            dag5.get_pipeline_output()
        expected_msg_components = ['cannot access the output of', 'either explicitly running task or setting a target']  # noqa: E501
        for comp in expected_msg_components:
            self.assertTrue(comp in str(cm.exception))

        # Get output of a task with a target (without running pipeline)
        dag5.run()
        output = dag5.get_pipeline_output()
        expected_output = 'Hello from task 1!\nHello from task 2!\nHello from task 3!\nHello from task 4!'  # noqa: E501
        self.assertEqual(expected_output, output)

        # Remove compiled directory and outputs, if they exist
        self._remove_compiled_dir(P005_SIMPLE_PROJECT_NO_NULL)

        # Set up directory for next test
        self._set_up_wkdir()
