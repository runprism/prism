"""
Load hooks unit test

Table of Contents:
- Imports
- Test case class definition
"""

###########
# Imports #
###########

# Standard library imports
import os
from pathlib import Path

# Prism imports
from prism.infra.hooks import PrismHooks, load_hooks
from prism.profiles import snowflake, pyspark, postgres
from prism.tests.integration import integration_test_class

# Paths
TEST_PROJECTS = Path(__file__).parent / 'test_projects'


##############################
# Test case class definition #
##############################

class TestLoadHooks(integration_test_class.IntegrationTestCase):

    def test_load_hooks_in_project(self):
        """
        `load_hook` produces a PrismHooks object when the user is in a project directory
        """
        PROJECT_PATH = TEST_PROJECTS / '013_hooks_sql_spark'
        os.chdir(PROJECT_PATH)

        # Load hooks
        hooks = load_hooks()
        adapter_dict = hooks.project.adapters_object_dict

        # Test
        self.assertTrue(isinstance(hooks, PrismHooks))
        self.assertTrue("snowflake_base" in adapter_dict.keys())
        self.assertTrue("pyspark_base" in adapter_dict.keys())
        self.assertTrue("postgres_base" in adapter_dict.keys())

        self.assertTrue(isinstance(adapter_dict["snowflake_base"], snowflake.Snowflake))
        self.assertTrue(isinstance(adapter_dict["pyspark_base"], pyspark.Pyspark))
        self.assertTrue(isinstance(adapter_dict["postgres_base"], postgres.Postgres))

        # Cleanup
        hooks.project.cleanup(hooks.project.run_context)
        self._set_up_wkdir()

    def test_load_hooks_not_in_project(self):
        """
        `load_hook` produces a PrismHooks object when the user is in a project directory
        """
        PROJECT_PATH = TEST_PROJECTS / '005_simple_project_no_null_tasks'
        os.chdir(PROJECT_PATH)

        # Load hooks
        hooks = load_hooks()
        self.assertTrue(isinstance(hooks, PrismHooks))
        self.assertEqual({}, hooks.project.adapters_object_dict)

        # Cleanup
        hooks.project.cleanup(hooks.project.run_context)
        self._set_up_wkdir()
