"""
Unit testing for agent-related functions

Table of Contents:
- Imports
- Test case directory and paths
- Run task instance to call functions
- Test case class definition
"""

###########
# Imports #
###########

# Standard library imports
import argparse
import os
from pathlib import Path
import unittest
from typing import Any, Dict
import pytest
import sys

# Prism imports
from prism.cli.agent import AgentTask
from prism.infra.project import PrismProject
from prism.agents.docker_agent import Docker
import prism.constants
import prism.exceptions


# Skip these tests if we're running in Windows.
if sys.platform.startswith("win"):
    pytest.skip("Skipping agent tests on Windows", allow_module_level=True)


#################################
# Test case directory and paths #
#################################

# Directory containing all prism_project.py test cases
TEST_CASE_WKDIR = os.path.dirname(__file__)
TEST_AGENT = Path(TEST_CASE_WKDIR) / 'test_agent'


#################
# Dummy classes #
#################

# Agent task
args = argparse.Namespace(log_level="ERROR")
agent_task = AgentTask(args)

# Prism proejct
project = PrismProject(
    project_dir=TEST_AGENT,
    user_context={},
    which="agent-run"
)
project.setup()


##################
# Util functions #
##################

# Agent objects
def _create_docker_agent(filename: str, conf: Dict[str, Any]):
    return Docker(
        args=args,
        agent_dir=TEST_AGENT,
        agent_filename=filename,
        agent_conf=conf,
        project=project,
        mode="test"
    )


def _create_valid_docker_agent():
    """
    Create a Docker agent using the valid configuration
    """
    # Agent YAML is valid
    agent_fname = 'valid_docker.yml'
    agent_yml = agent_task.load_agent_yml(TEST_AGENT / agent_fname, project)
    conf = agent_task.is_valid_agent_yml(agent_fname, agent_yml)

    # Agent configuration is valid (this function is automatically called on object
    # instantiation).
    return _create_docker_agent(agent_fname, conf)


def _remove_logs():
    if Path(Path(TEST_CASE_WKDIR) / 'logs.log').is_file():
        os.unlink(Path(TEST_CASE_WKDIR) / 'logs.log')


##############################
# Test case class definition #
##############################

class TestAgent(unittest.TestCase):

    def test_valid_docker_agent_yml_conf(self):
        """
        A valid agent YAML configuration is loaded as expected
        """
        _create_valid_docker_agent()
        _remove_logs()

    def test_invalid_docker_agent_yml(self):
        """
        The following configurations produce an error:
        - A bad top-level key (i.e., anything other than `agent`)
        - Too many top-level keys
        - A missing `type`
        """

        # Filename, expected message dictionary
        fname_expected_msg_dict = {
            'invalid_docker_badkey.yml': "invalid top-level key in `invalid_docker_badkey.yml`... should be `agents`",  # noqa: E501
            'invalid_docker_toomanykeys.yml': "more than one top-level key in `invalid_docker_toomanykeys.yml`",  # noqa: E501
            'invalid_docker_notype.yml': "`type` not in agent `invalid_docker_notype.yml` configuration"  # noqa: E501
        }

        # Iterate through dictionary
        for fname, msg in fname_expected_msg_dict.items():
            agent_yml = agent_task.load_agent_yml(TEST_AGENT / fname, project)
            if fname == "invalid_docker_notype.yml":
                with self.assertRaises(prism.exceptions.InvalidAgentsConfException) as cm:  # noqa: E501
                    agent_task.is_valid_agent_yml(fname, agent_yml)
            else:
                with self.assertRaises(prism.exceptions.InvalidAgentsYmlException) as cm:  # noqa: E501
                    agent_task.is_valid_agent_yml(fname, agent_yml)
            self.assertEqual(msg, str(cm.exception))

        _remove_logs()

    def test_invalid_docker_agent_confs(self):
        """
        The following configurations should produce an error:
        - No `type` (this is technically raised when the YAML itself is loaded)
        - No `requirements`
        - Bad argument types
        """
        # All invalid configurations
        agent_fname = 'invalid_docker_confs.yml'
        all_invalid_confs = agent_task.load_agent_yml(
            TEST_AGENT / agent_fname, project
        )

        # No `requirements`
        no_reqs_conf = all_invalid_confs["agent_no_reqs"]
        with self.assertRaises(prism.exceptions.InvalidAgentsConfException) as cm:
            _ = _create_docker_agent(agent_fname, no_reqs_conf)
        self.assertEqual(
            "`requirements` not in agent `invalid_docker_confs` configuration",
            str(cm.exception)
        )

        # Bad argument types
        for key in ["image", "server_url", "env"]:
            conf = all_invalid_confs[f"agent_bad_{key}_arg"]
            with self.assertRaises(prism.exceptions.InvalidAgentsConfException) as cm:
                _ = _create_docker_agent(agent_fname, conf)
            self.assertEqual(
                f"`{key}` is not the correct type",
                str(cm.exception)
            )

        _remove_logs()

    def test_parse_requirements_exists_noexists(self):
        """
        Configurations that specify a requirements.txt path that does not exist should
        produce an error.
        """
        # All invalid configurations
        invalid_agent_fname = 'invalid_docker_confs.yml'
        all_invalid_confs = agent_task.load_agent_yml(
            TEST_AGENT / invalid_agent_fname, project
        )

        # Requirements path exists
        valid_agent_fname = 'valid_docker_reqs.yml'
        reqs_exists_yml = agent_task.load_agent_yml(
            TEST_AGENT / valid_agent_fname, project
        )
        reqs_exists_conf = agent_task.is_valid_agent_yml(
            valid_agent_fname,
            reqs_exists_yml
        )
        agent = _create_docker_agent(valid_agent_fname, reqs_exists_conf)
        reqs_path = agent.parse_requirements(agent.agent_conf)
        self.assertEqual(TEST_AGENT / 'reqs_test' / 'requirements.txt', reqs_path)

        # Requirements path doesn't exist
        reqs_path_no_exists_conf = all_invalid_confs["agent_reqs_do_not_exist"]
        agent = _create_docker_agent(invalid_agent_fname, reqs_path_no_exists_conf)
        with self.assertRaises(prism.exceptions.FileNotFoundException) as cm:
            _ = agent.parse_requirements(reqs_path_no_exists_conf)
        expected_msg = f"no file found at {str(TEST_AGENT / 'requirements_does_not_exist.txt')}"  # noqa E501
        self.assertEqual(expected_msg, str(cm.exception))

        _remove_logs()

    def test_parse_profile_paths(self):
        """
        When the user specifies a BigQuery and/or a dbt profile, the Docker agent needs
        to identify these paths are generate COPY commands for them.
        """
        # Create a Docker agent using the valid configuration
        agent = _create_valid_docker_agent()
        profile_paths = agent.parse_profile_paths(agent.project)

        # Expected paths
        creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        dbt_project_dir = Path(TEST_CASE_WKDIR).parent \
            / 'integration' \
            / 'test_projects' \
            / '009_simple_dbt_project' \
            / 'jaffle_shop'

        self.assertEqual(set(profile_paths), set([str(creds), str(dbt_project_dir)]))

        _remove_logs()

    def test_prepare_paths_for_copy(self):
        """
        The Docker agent requires that paths relevant to the user's project get copied
        into the image.
        """
        # Create a Docker agent using the valid configuration
        agent = _create_valid_docker_agent()
        all_copy_paths = agent.prepare_paths_for_copy(agent.project, "")

        # Expected copy paths
        dbt_project_dir = str(
            Path(TEST_CASE_WKDIR).parent
            / 'integration'  # noqa: W503
            / 'test_projects'  # noqa: W503
            / '009_simple_dbt_project'  # noqa: W503
            / 'jaffle_shop'  # noqa: W503
        )

        expected_paths = [
            TEST_AGENT,  # project directory
            TEST_AGENT / 'paths_test',  # profile directory
            Path(TEST_CASE_WKDIR) / 'test_trigger_yml',  # in sys.path.conf
            os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),  # from profile
            dbt_project_dir  # from profile
        ]
        for e in expected_paths:
            self.assertTrue(e in all_copy_paths.keys())

            # The copy statement excludes the first forward slash
            self.assertEqual(all_copy_paths[e], f"COPY {str(e)[1:]} .{e}")

        # There should not be any paths in `all_copy_paths` that were not tested above
        self.assertEqual(len(expected_paths), len(all_copy_paths.keys()))

        _remove_logs()

    def test_parse_base_image(self):
        """
        Parse the base image if it exists, otherwise, default to
        prism.constants.DEFAULT_DOCKER_IMAGE.
        """
        # Image when `image` is not specified
        agent = _create_valid_docker_agent()
        self.assertEqual(
            prism.constants.DEFAULT_DOCKER_IMAGE,
            agent.parse_base_image(agent.agent_conf)
        )

        # Image when `image` is specified
        agent_fname = "valid_docker_image.yml"
        agent_conf = agent_task.load_agent_conf(
            TEST_AGENT / agent_fname,
            agent_fname,
            project
        )
        agent = _create_docker_agent(
            agent_fname,
            agent_conf
        )
        self.assertEqual(
            "base_python",
            agent.parse_base_image(agent.agent_conf)
        )

        _remove_logs()

    def test_parse_docker_environment_variables(self):
        """
        Parsed environment variables are a dictionary
        """
        agent = _create_valid_docker_agent()
        self.assertEqual(
            {"env_var_1": "value1"},
            agent.parse_environment_variables(agent.agent_conf),
        )

        _remove_logs()


# Cleanup
project.cleanup(project.run_context)
