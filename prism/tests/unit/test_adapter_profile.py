"""
Unit testing for profiles and adapters.

Table of Contents:
- Imports
- Test case directory and paths
- Task instances to call functions
- Test case class definition
"""

#############
## Imports ##
#############

# Standard library imports
import jinja2
import os
from pathlib import Path
import unittest

# Profile imports
import snowflake.connector
from pyspark.sql import SparkSession

# Prism imports
import prism.exceptions
from prism.cli import run, spark_submit
from prism.parsers import yml_parser
import prism.profiles
from prism.profiles import meta, adapter, snowflake as prism_snowflake, pyspark as prism_pyspark
import prism.profiles.profile as pr


###################################
## Test case directory and paths ##
###################################

# Directory containing test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
PROFILE_YML_TEST_CASES = Path(TEST_CASE_WKDIR) / 'test_profile_yml' / 'profile.yml'
PROFILE_YML_EMPTY = Path(TEST_CASE_WKDIR) / 'test_profile_yml' / 'profile_empty.yml'


####################
## Util functions ##
####################

def _load_profile_yml(
    profiles_path: Path
) -> dict:
    """
    Load profile.yml file

    args:
        profiles_path: path to profile.yml
    returns:
        profile_yml: profile.yml file represented as a dict
    """
    try:
        parser = yml_parser.YamlParser(profiles_path)
        profile_yml = parser.parse()
        return profile_yml
    
    # If template isn't found, return an empty dictionary
    except jinja2.exceptions.TemplateNotFound:
        return {}
    
    # Raise all other exceptions
    except Exception as e:
        raise e


def _load_named_profile(
    profile_yml: dict,
    profile_name: str
):
    return profile_yml[profile_name]
    

def _load_named_profile_adapters(
    profile_yml: dict,
    profile_name: str
):
    return profile_yml[profile_name]['adapters']


######################
## Load profile.yml ##
######################

profile_yml_tests = _load_profile_yml(PROFILE_YML_TEST_CASES)
profile_yml_empty = _load_profile_yml(PROFILE_YML_EMPTY)


######################
## Class definition ##
######################


class TestAdapter(unittest.TestCase):
    
    def test_normal_snowflake_adapter(self):
        """
        Snowflake adapter class (and functions) behave as expected for normal snowflake profile
        """
        profile_name = 'profile_snowflake_normal'
        adapter_dict = _load_named_profile_adapters(profile_yml_tests, profile_name)['snowflake']
        snowflake_adapter = prism_snowflake.Snowflake('snowflake', adapter_dict, False)

        # Get config dictionary and config variables
        self.assertTrue(snowflake_adapter.is_valid_config(adapter_dict))
        for var in ['SNOWFLAKE_USER','SNOWFLAKE_PASSWORD','SNOWFLAKE_ACCOUNT','SNOWFLAKE_ROLE','SNOWFLAKE_WAREHOUSE','SNOWFLAKE_DATABASE','SNOWFLAKE_SCHEMA']:
            self.assertEqual(os.getenv(var), adapter_dict[var.split('_')[1].lower()])
        
        # get_adapter_var throws an error for missing variable
        with self.assertRaises(prism.exceptions.InvalidProfileException) as cm:
            snowflake_adapter.get_adapter_var(adapter_dict, 'this_does_not_exist', 'snowflake')
        self.assertEqual("`this_does_not_exist` not found in `snowflake` profile in profile.yml", str(cm.exception))
    

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
        for p in profiles:
            adapter_dict = _load_named_profile_adapters(profile_yml_tests, p)['snowflake']
            snowflake_adapter = prism_snowflake.Snowflake('snowflake', adapter_dict, False)
        
            # Config dictionaries are invalid
            with self.assertRaises(prism.exceptions.InvalidProfileException) as cm:
                snowflake_adapter.is_valid_config(adapter_dict)
            msgs[p] = str(cm.exception)
        
        self.assertEqual(msgs['profile_snowflake_none_config'], 'var `schema` under snowflake config cannot be None in profile.yml')
        self.assertEqual(msgs['profile_snowflake_missing_config'], 'need to define `database` under snowflake config in profile.yml')
        self.assertEqual(msgs['profile_snowflake_extra_config'], 'invalid var `extra_config` under snowflake config in profile.yml')

    
    def test_normal_pyspark_adapter(self):
        """
        PySpark adapter class (and functions) behave as expected for normal pyspark profile
        """
        profile_name = 'profile_pyspark_normal'
        adapter_dict = _load_named_profile_adapters(profile_yml_tests, profile_name)['pyspark']
        pyspark_adapter = prism_pyspark.Pyspark('pyspark', adapter_dict, False)

        # Get config dictionary and config variables
        pyspark_alias = pyspark_adapter.get_alias()
        pyspark_config = pyspark_adapter.get_adapter_var(adapter_dict, 'config', 'pyspark')
        self.assertEqual('spark', pyspark_alias)
        self.assertEqual(4, pyspark_config['spark.driver.cores'])
        self.assertEqual('10g', pyspark_config['spark.driver.memory'])
        self.assertEqual(None, pyspark_config['spark.sql.broadcastTimeout'])


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
        for p in profiles:
            adapter_dict = _load_named_profile_adapters(profile_yml_tests, p)['pyspark']
            pyspark_adapter = prism_pyspark.Pyspark('pyspark', adapter_dict, False)
            config = pyspark_adapter.get_adapter_var(adapter_dict, 'config', 'pyspark')
        
            # Config dictionaries are invalid
            with self.assertRaises(prism.exceptions.InvalidProfileException) as cm:
                pyspark_adapter.get_alias()
            msgs[p] = str(cm.exception)
        
        self.assertEqual(msgs['profile_pyspark_none_alias'], '`alias` cannot be None in `pyspark` profile in profile.yml')
        self.assertEqual(msgs['profile_pyspark_missing_alias'], '`alias` not found in `pyspark` profile in profile.yml')


class TestProfile(unittest.TestCase):


    def test_profile_bad_top_level_keys(self):
        """
        profile instantiation throws an error if top-level keys are misspecified or bad
        """
        # If profile doesn't exist, then the profile instantiation should throw an error
        error_top_level_keys_profiles = {
            "profile_too_many_toplevel_keys": '',
            "profile_invalid_toplevel_key": " `['this_does_not_belong']`"
        }
        for k,v in error_top_level_keys_profiles.items():
            with self.assertRaises(prism.exceptions.InvalidProfileException) as cm:
                pr.Profile(profile_yml_tests, k, env="local")
            msg_list = [
                'invalid keys in profile.yml' + v,
                'should only be `adapters` and `clusters`',
            ]
            self.assertEqual('\n'.join(msg_list), str(cm.exception))


    def test_normal_snowflake_adapter(self):
        """
        Profile creates Snowflake engine as expected 
        """
        profile_name = "profile_snowflake_normal"
        profile = pr.Profile(profile_yml_tests, profile_name, env="local")
        self.assertEqual(profile.adapters_obj_dict, {})
        profile.generate_adapters()
        expected_keys = ['snowflake']
        self.assertEqual(expected_keys, list(profile.adapters_obj_dict.keys()))
        snowflake_adapter = profile.adapters_obj_dict['snowflake']
        self.assertTrue(isinstance(snowflake_adapter, prism_snowflake.Snowflake))
        self.assertTrue(isinstance(snowflake_adapter.engine, snowflake.connector.SnowflakeConnection))


    def test_normal_pyspark_adapter(self):
        """
        Profile creates PySpark engine as expected
        """
        profile_name = "profile_pyspark_normal"
        profile = pr.Profile(profile_yml_tests, profile_name, env="local")
        self.assertEqual(profile.adapters_obj_dict, {})
        profile.generate_adapters()
        expected_keys = ['pyspark']
        self.assertEqual(expected_keys, list(profile.adapters_obj_dict.keys()))
        pyspark_adapter = profile.adapters_obj_dict['pyspark']
        self.assertTrue(isinstance(pyspark_adapter, prism_pyspark.Pyspark))
        self.assertTrue(isinstance(pyspark_adapter.engine, SparkSession))


    def test_multiple_adapters(self):
        """
        Profile correctly multiple engines when multiple adapters are specified
        """
        profile_name = "profile_snowflake_pyspark_normal"
        profile = pr.Profile(profile_yml_tests, profile_name, env="local")
        self.assertEqual(profile.adapters_obj_dict, {})
        profile.generate_adapters()
        expected_keys = ['snowflake', 'pyspark']
        self.assertEqual(expected_keys, list(profile.adapters_obj_dict.keys()))
        snowflake_adapter = profile.adapters_obj_dict['snowflake']
        pyspark_adapter = profile.adapters_obj_dict['pyspark']
        self.assertTrue(isinstance(snowflake_adapter, prism_snowflake.Snowflake))
        self.assertTrue(isinstance(pyspark_adapter, prism_pyspark.Pyspark))
        self.assertTrue(isinstance(snowflake_adapter.engine, snowflake.connector.SnowflakeConnection))
        self.assertTrue(isinstance(pyspark_adapter.engine, SparkSession))


    def invalid_adapter(self):
        """
        Profile throws an error if asked to create unsupported adapter
        """
        profile_name = "profile_extra_invalid_connection"
        profile = pr.Profile(profile_yml_tests, profile_name, env="local")
        self.assertEqual(profile.adapters_obj_dict, {})
        with self.assertRaises(prism.exceptions.InvalidProfileException) as cm:
            profile.generate_adapters()
        self.assertEqual('invalid adapter `postgres_sql` in profile.yml', str(cm.exception))


    #################
    # Miscellaneous #
    #################

    def test_missing_profile_yml(self):
        """
        Attempting to load a non-existent profile.yml should produce an empty dictionary. Note that _load_profile_yml
        is identical to the load_profile_yml method in the Project class.
        """
        nonexistent_profile = _load_profile_yml(Path(TEST_CASE_WKDIR) / 'test_profile_yml' / 'error.yml')
        self.assertEqual({}, nonexistent_profile)
        

    def test_empty_profile_yml(self):
        """
        Loading an empty profile.yml file should produce an empty dictionary
        """
        self.assertEqual({}, profile_yml_empty)


    def test_empty_named_profile(self):
        """
        An empty named profile doesn't throw an error, but does cause Profile.bool_all_profiles_exist to be False
        """
        profile = pr.Profile(profile_yml_tests, "profile_empty", env="local")
        self.assertFalse(profile.bool_all_profiles_exist)


# EOF