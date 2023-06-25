"""
Unit testing for profiles and adapters.

Table of Contents:
- Imports
- Test case directory and paths
- Task instances to call functions
- Test case class definition
"""

###########
# Imports #
###########

# Standard library imports
from dataclasses import dataclass
import jinja2
import os
from pathlib import Path
import unittest

# Profile imports
import snowflake.connector
from pyspark.sql import SparkSession

# Prism imports
import prism.exceptions
from prism.parsers import yml_parser
import prism.profiles
from prism.profiles import (  # noqa: F401
    meta,
    adapter,
    snowflake as prism_snowflake,
    pyspark as prism_pyspark
)
import prism.profiles.profile as pr
import prism.prism_logging


#################################
# Test case directory and paths #
#################################

# Directory containing test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
PROFILE_YML_TEST_CASES = Path(TEST_CASE_WKDIR) / 'test_profile_yml' / 'profile.yml'
PROFILE_YML_EMPTY = Path(TEST_CASE_WKDIR) / 'test_profile_yml' / 'profile_empty.yml'


##################
# Util functions #
##################

def _load_profile_yml(
    profile_yml_path: Path
) -> dict:
    """
    Load profile YML file

    args:
        profile_yml_path: path to profile YML
    returns:
        profile_yml: profile YML file represented as a dict
    """
    try:
        parser = yml_parser.YamlParser(profile_yml_path)
        profile_yml = parser.parse()
        return profile_yml

    # If template isn't found, return an empty dictionary
    except jinja2.exceptions.TemplateNotFound:
        return {}

    # Raise all other exceptions
    except Exception as e:
        raise e


def _load_named_profile_adapters(
    profile_yml: dict,
    profile_name: str
):
    return profile_yml[profile_name]['adapters']


####################
# Load profile YML #
####################

profile_yml_tests = _load_profile_yml(PROFILE_YML_TEST_CASES)
profile_yml_empty = _load_profile_yml(PROFILE_YML_EMPTY)


# We need to set up the default logger so that some warnings will fire
@dataclass
class LoggingArgs:
    log_level: str = 'info'


prism.prism_logging.set_up_logger(LoggingArgs())


def _remove_logs():
    if Path(Path(__file__).parent / 'logs.log').is_file():
        os.unlink(Path(__file__).parent / 'logs.log')


####################
# Class definition #
####################


class TestAdapter(unittest.TestCase):

    def test_normal_snowflake_adapter(self):
        """
        Snowflake adapter class (and functions) behave as expected for normal snowflake
        profile
        """
        profile_name = 'profile_snowflake_normal'
        adapter_name = 'snowflake'
        adapter_dict = _load_named_profile_adapters(
            profile_yml_tests, profile_name
        )['snowflake']
        snowflake_adapter = prism_snowflake.Snowflake(
            adapter_name, adapter_dict, profile_name, False
        )

        # Get config dictionary and config variables
        self.assertTrue(
            snowflake_adapter.is_valid_config(adapter_dict, adapter_name, profile_name)
        )
        for var in [
            'SNOWFLAKE_USER',
            'SNOWFLAKE_PASSWORD',
            'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_ROLE',
            'SNOWFLAKE_WAREHOUSE',
            'SNOWFLAKE_DATABASE',
            'SNOWFLAKE_SCHEMA'
        ]:
            self.assertEqual(os.getenv(var), adapter_dict[var.split('_')[1].lower()])

        # get_adapter_var throws an error for missing variable
        with self.assertRaises(prism.exceptions.InvalidProfileException) as cm:
            snowflake_adapter.get_adapter_var(
                adapter_dict, 'this_does_not_exist', adapter_name, profile_name
            )
        self.assertEqual(
            f"`this_does_not_exist` not found - see `{adapter_name}` adapter in `{profile_name}` profile in profile YML",  # noqa: E501
            str(cm.exception)
        )

        _remove_logs()

    def test_error_snowflake_adapter(self):
        """
        Snowflake adapter class (and functions) throw expected errors
        """
        msgs = {}
        profiles = [
            'profile_snowflake_none_config',
            'profile_snowflake_missing_config',
            'profile_snowflake_extra_config'
        ]

        # Adapter class can be still be instantiated
        adapter_name = 'snowflake'
        for p in profiles:
            adapter_dict = _load_named_profile_adapters(
                profile_yml_tests, p
            )['snowflake']
            snowflake_adapter = prism_snowflake.Snowflake(
                adapter_name, adapter_dict, p, False
            )

            # Config dictionaries are invalid
            with self.assertRaises(prism.exceptions.InvalidProfileException) as cm:
                snowflake_adapter.is_valid_config(adapter_dict, adapter_name, p)
            msgs[p] = str(cm.exception)

        self.assertEqual(
            msgs['profile_snowflake_none_config'],
            f'`schema` cannot be None - see `{adapter_name}` adapter in `profile_snowflake_none_config` profile in profile YML'  # noqa: E501
        )
        self.assertEqual(
            msgs['profile_snowflake_missing_config'],
            f'`database` must be defined - see `{adapter_name}` adapter in `profile_snowflake_missing_config` profile in profile YML'  # noqa: E501
        )
        self.assertEqual(
            msgs['profile_snowflake_extra_config'],
            f'invalid var `extra_config` - see `{adapter_name}` adapter in `profile_snowflake_extra_config` profile in profile YML'  # noqa: E501
        )

        _remove_logs()

    def test_normal_pyspark_adapter(self):
        """
        PySpark adapter class (and functions) behave as expected for normal pyspark
        profile
        """
        profile_name = 'profile_pyspark_normal'
        adapter_name = 'pyspark'
        adapter_dict = _load_named_profile_adapters(
            profile_yml_tests, profile_name
        )['pyspark']
        pyspark_adapter = prism_pyspark.Pyspark(
            adapter_name, adapter_dict, profile_name, False
        )

        # Get config dictionary and config variables
        pyspark_alias = pyspark_adapter.get_alias()
        pyspark_config = pyspark_adapter.get_adapter_var(
            adapter_dict, 'config', adapter_name, profile_name
        )
        self.assertEqual('spark', pyspark_alias)
        self.assertEqual(4, pyspark_config['spark.driver.cores'])
        self.assertEqual('10g', pyspark_config['spark.driver.memory'])
        self.assertEqual(None, pyspark_config['spark.sql.broadcastTimeout'])

        _remove_logs()

    def test_error_pyspark_adapter(self):
        """
        Pyspark adapter class (and functions) throw expected errors
        """
        msgs = {}
        profiles = [
            'profile_pyspark_none_alias',
            'profile_pyspark_missing_alias'
        ]

        # Adapter class can be still be instantiated
        adapter_name = 'pyspark'
        for p in profiles:
            adapter_dict = _load_named_profile_adapters(profile_yml_tests, p)['pyspark']
            pyspark_adapter = prism_pyspark.Pyspark(
                adapter_name, adapter_dict, p, False
            )
            _ = pyspark_adapter.get_adapter_var(
                adapter_dict, 'config', adapter_name, p
            )

            # Config dictionaries are invalid
            with self.assertRaises(prism.exceptions.InvalidProfileException) as cm:
                pyspark_adapter.get_alias()
            msgs[p] = str(cm.exception)

        self.assertEqual(
            msgs['profile_pyspark_none_alias'],
            f'`alias` cannot be None - see `{adapter_name}` adapter in `profile_pyspark_none_alias` profile in profile YML'  # noqa: E501
        )
        self.assertEqual(
            msgs['profile_pyspark_missing_alias'],
            f'`alias` not found - see `{adapter_name}` adapter in `profile_pyspark_missing_alias` profile in profile YML'  # noqa: E501
        )

        _remove_logs()


class TestProfile(unittest.TestCase):

    def test_profile_bad_top_level_keys(self):
        """
        profile instantiation throws an error if top-level keys are misspecified or bad
        """
        with self.assertRaises(prism.exceptions.InvalidProfileException) as cm:
            pr.Profile(profile_yml_tests, "profile_too_many_toplevel_keys")
        msg = "invalid keys `['clusters', 'this_does_not_belong']` in profile YML; supported keys are [`adapters`]"  # noqa: E501
        self.assertEqual(msg, str(cm.exception))

        _remove_logs()

    def test_normal_snowflake_adapter(self):
        """
        Profile creates Snowflake engine as expected
        """
        profile_name = "profile_snowflake_normal"
        profile = pr.Profile(profile_yml_tests, profile_name)
        self.assertEqual(profile.adapters_obj_dict, {})
        profile.generate_adapters()
        expected_keys = ['snowflake']
        self.assertEqual(expected_keys, list(profile.adapters_obj_dict.keys()))
        snowflake_adapter = profile.adapters_obj_dict['snowflake']
        self.assertTrue(isinstance(snowflake_adapter, prism_snowflake.Snowflake))
        self.assertTrue(isinstance(snowflake_adapter.engine, snowflake.connector.SnowflakeConnection))  # noqa: E501

        _remove_logs()

    def test_multiple_snowflake_adapters(self):
        """
        Profile creates multiple Snowflake engines as expected
        """
        profile_name = "profile_multiple_snowflakes"
        profile = pr.Profile(profile_yml_tests, profile_name)
        self.assertEqual(profile.adapters_obj_dict, {})
        profile.generate_adapters()
        expected_keys = ['snowflake1', 'snowflake2']
        self.assertEqual(expected_keys, list(profile.adapters_obj_dict.keys()))
        for key in ['snowflake1', 'snowflake2']:
            snowflake_adapter = profile.adapters_obj_dict[key]
            self.assertTrue(isinstance(snowflake_adapter, prism_snowflake.Snowflake))
            self.assertTrue(isinstance(snowflake_adapter.engine, snowflake.connector.SnowflakeConnection))  # noqa: E501

        # Confirm that the two adapters are distinct
        self.assertNotEqual(
            profile.adapters_obj_dict['snowflake1'],
            profile.adapters_obj_dict['snowflake2']
        )

        _remove_logs()

    def test_normal_pyspark_adapter(self):
        """
        Profile creates PySpark engine as expected
        """
        profile_name = "profile_pyspark_normal"
        profile = pr.Profile(profile_yml_tests, profile_name)
        self.assertEqual(profile.adapters_obj_dict, {})
        profile.generate_adapters()
        expected_keys = ['pyspark']
        self.assertEqual(expected_keys, list(profile.adapters_obj_dict.keys()))
        pyspark_adapter = profile.adapters_obj_dict['pyspark']
        self.assertTrue(isinstance(pyspark_adapter, prism_pyspark.Pyspark))
        self.assertTrue(isinstance(pyspark_adapter.engine, SparkSession))

        _remove_logs()

    def test_multiple_adapters(self):
        """
        Profile correctly multiple engines when multiple adapters are specified
        """
        profile_name = "profile_snowflake_pyspark_normal"
        profile = pr.Profile(profile_yml_tests, profile_name)
        self.assertEqual(profile.adapters_obj_dict, {})
        profile.generate_adapters()
        expected_keys = ['snowflake', 'pyspark']
        self.assertEqual(expected_keys, list(profile.adapters_obj_dict.keys()))
        snowflake_adapter = profile.adapters_obj_dict['snowflake']
        pyspark_adapter = profile.adapters_obj_dict['pyspark']
        self.assertTrue(isinstance(snowflake_adapter, prism_snowflake.Snowflake))
        self.assertTrue(isinstance(pyspark_adapter, prism_pyspark.Pyspark))
        self.assertTrue(isinstance(snowflake_adapter.engine, snowflake.connector.SnowflakeConnection))  # noqa: E501
        self.assertTrue(isinstance(pyspark_adapter.engine, SparkSession))

        _remove_logs()

    def invalid_adapter(self):
        """
        Profile throws an error if asked to create unsupported adapter
        """
        profile_name = "profile_extra_invalid_connection"
        profile = pr.Profile(profile_yml_tests, profile_name)
        self.assertEqual(profile.adapters_obj_dict, {})
        with self.assertRaises(prism.exceptions.InvalidProfileException) as cm:
            profile.generate_adapters()
        self.assertEqual(
            'invalid adapter `postgres_sql` in profile YML',
            str(cm.exception)
        )

        _remove_logs()

    #################
    # Miscellaneous #
    #################

    def test_missing_profile_yml(self):
        """
        Attempting to load a non-existent profile YML should produce an empty
        dictionary. Note that _load_profile_yml is identical to the load_profile_yml
        method in the Project class.
        """
        nonexistent_profile = _load_profile_yml(
            Path(TEST_CASE_WKDIR) / 'test_profile_yml' / 'error.yml'
        )
        self.assertEqual({}, nonexistent_profile)

        _remove_logs()

    def test_empty_profile_yml(self):
        """
        Loading an empty profile YML file should produce an empty dictionary
        """
        self.assertEqual({}, profile_yml_empty)

        _remove_logs()

    def test_empty_named_profile(self):
        """
        An empty named profile doesn't throw an error, but does cause
        Profile.bool_all_profiles_exist to be False
        """
        profile = pr.Profile(profile_yml_tests, "profile_empty")
        self.assertFalse(profile.bool_all_profiles_exist)

        _remove_logs()

    # TODO: test the BigQuery and Redshift adapters
