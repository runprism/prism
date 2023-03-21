"""
Docker Agent.
"""


###########
# Imports #
###########

# Prism imports
from prism.agents.base import Agent
import prism.constants
import prism.event_managers.base
import prism.exceptions
from prism.infra.project import PrismProject
import prism.logging

# Standard library imports
import argparse
import docker
import os
from pathlib import Path
import re
from typing import Any, Dict, Optional, Union
from tempfile import TemporaryDirectory
import shutil


#################
# Docker client #
#################

client = docker.from_env()


####################
# Class definition #
####################

class Docker(Agent):

    def __init__(self,
        args: argparse.Namespace,
        agent_dir: Path,
        agent_filename: str,
        agent_conf: Dict[str, Any],
        project: PrismProject
    ):
        self.image = None
        super().__init__(args, agent_dir, agent_filename, agent_conf, project)

    def is_valid_conf(self, agent_conf: Dict[str, Any]):
        """
        A Docker agent should be formatted as follows:

        agent:
          type: docker
          image: <base image name>
          requirements: <path to requirements.txt>
          env:
            var1: value1
            var2: value2
        ...
        """
        required_keys = {
            "type": str,
            "requirements": str,
        }
        optional_keys = {
            "image": str,
            "env": dict
        }

        # Check required keys
        for _key, _type in required_keys.items():
            if _key not in list(agent_conf.keys()):
                raise prism.exceptions.InvalidAgentsConfException(
                    message=f"`{_key}` not in agent `{self.agent_name}` configuration"
                )

            if not isinstance(agent_conf[_key], _type):
                raise prism.exceptions.InvalidAgentsConfException(
                    message=f"`{_key}` is not the correct type"
                )

        # Check optional keys, if they exist
        for _key, _type in optional_keys.items():
            if _key in list(agent_conf.keys()):
                if not isinstance(agent_conf[_key], _type):
                    raise prism.exceptions.InvalidAgentsConfException(
                        message=f"`{_key}` is not the correct type"
                    )

        # If no exception has been raised, return True
        return True

    def _copy_file_dir(self,
        src: Optional[Union[str, Path]],
        target: Path,
    ):
        """
        Copy directory from source to target

        args:
            src: source directory to copy
            target: target location of directory
        returns:
            None
        """
        if src is None:
            return
        if Path(src).is_dir() and not target.is_dir():
            shutil.copytree(
                src,
                target
            )
        elif Path(src).is_file() and not target.is_file():
            # Make the parent directory first
            if not os.path.exists(Path(target).parent):
                os.makedirs(Path(target.parent), exist_ok=True)

            # Copy file
            shutil.copyfile(
                src,
                target
            )
        return

    def parse_profile_paths(self,
        project: PrismProject
    ):
        """
        Certain profiles require the user to point to specific files or directories. If
        these profiles exist in the user's project, then we need to parse those paths
        and copy them over to our agent's computing environment.
        """
        # Get the project's named profile
        profile_yml = project.profile_yml
        named_profile = {}
        for k, v in profile_yml.items():
            if k == project.profile_name:
                if v is not None:
                    named_profile = v

        # The only profiles that specify paths are the dbt and bigquery profiles.
        paths = []
        try:
            adapters = named_profile["adapters"]
            for _, adapter_conf in adapters.items():
                if adapter_conf["type"] == 'dbt':
                    paths.append(adapter_conf["project_dir"])
                    paths.append(adapter_conf["profiles_dir"])
                if adapter_conf["type"] == "bigquery":
                    paths.append(adapter_conf["creds"])

            # Return
            return paths

        # For any error, just return. If there truly is an error in the project, then
        # it will be revealed when the user runs the project in the agent's computing
        # environment.
        except Exception:
            return paths

    def prepare_paths_for_copy(self,
        project: PrismProject,
        tmpdir: str
    ):
        """
        Prism projects often rely on more than just their own directory. They can import
        functions / modules or reference configuration files from other directories. We
        need to copy all relevant directories into our agent, and we need to ensure that
        these paths have the same relative location to the project directory.

        args:
            project: Prism project
            tmpdir: temporary directory in which to copy directories
        returns:
            ...
        """
        project_dir = project.project_dir
        sys_path_config = project.sys_path_config
        profiles_dir = project.profiles_dir
        triggers_dir = project.triggers_dir

        # Get paths of files / directories associated with profiles
        profile_paths = self.parse_profile_paths(project)

        # Copy commands
        copy_commands = {}

        # Copy directories into tmpdir
        for _dir in [profiles_dir, triggers_dir] + profile_paths:
            if _dir is None:
                continue

            # If the directory is equivalent to the project directory, also skip. We
            # automatically copy this into our Docker image.
            elif str(_dir) == str(project_dir):
                continue

            else:
                copy_commands[_dir] = f"COPY {_dir} ./{str(_dir)[1:]}"
                self._copy_file_dir(
                    _dir,
                    Path(tmpdir) / str(_dir)[1:]
                )
        for _dir in sys_path_config:
            if _dir is None:
                continue
            copy_commands[_dir] = f"COPY {str(_dir)[1:]} ./{str(_dir)[1:]}"
            self._copy_file_dir(
                _dir,
                Path(tmpdir) / str(_dir)[1:]
            )

        # Return copy commands
        return copy_commands

    def parse_base_image(self,
        agent_conf: Dict[str, Any]
    ) -> str:
        """
        Get the user-specified base image from the agent's configuration

        args:
            agent_conf: agent configuration as dictionary
        returns:
            `pip install` command for requirements
        """
        if "image" in agent_conf.keys():
            return agent_conf["image"]

        # If the user doesn't specify a base image, then use the default image specified
        # in the Prism constants module.
        else:
            return prism.constants.DEFAULT_DOCKER_IMAGE

    def parse_environment_variables(self,
        agent_conf: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Get environment variables from the agent's configuration and store in a
        dictionary

        args:
            agent_conf: agent configuration as dictionary
        returns:
            environment variables as a dictionary
        """
        if "env" in agent_conf.keys():
            env_vars = agent_conf["env"]
            return env_vars
        else:
            return {}

    def apply(self):
        """
        Create the Docker image
        """
        # Environment dictionary
        env_dict = self.parse_environment_variables(self.agent_conf)

        # requirements.txt path
        requirements_txt_path = Path(self.parse_requirements(self.agent_conf))

        # Update the version number
        image_name = f"prism-dockeragent-{self.agent_name}"
        old_img_version = None
        img_list = client.images.list()
        for img in img_list:
            if image_name in img.tags[0]:
                old_img_version = img.tags[0].split(":")[1]

        # If there is no image, then set the first image to be version 1.0
        if old_img_version is None:
            new_img_version = "1.0"
        else:
            new_img_version = str(round(float(old_img_version) + 0.1, 1))
        new_image_name = image_name + f":{new_img_version}"

        with TemporaryDirectory(prefix="docker") as tmpdir:
            # Copy project directory and requirements.txt file to parent of
            # newly-created NamedTemporaryFile. We do this because all paths in the
            # Docker build context have to be relative to the location of the Docker
            # build context itself.
            shutil.copytree(
                self.project.project_dir,
                Path(tmpdir) / str(self.project.project_dir)[1:]
            )
            shutil.copy(
                requirements_txt_path,
                Path(tmpdir) / str(requirements_txt_path)[1:]
            )

            # Copy other, related project files
            other_copy_commands_dict = self.prepare_paths_for_copy(
                self.project,
                tmpdir
            )
            other_copy_commands = '\n'.join([
                cmd for _, cmd in other_copy_commands_dict.items()
            ])

            # Populate build context template
            kwargs = {
                "base_image": self.parse_base_image(self.agent_conf),
                "requirements_txt_path": str(requirements_txt_path)[1:],
                "project_dir": str(self.project.project_dir)[1:],
                "other_copy_commands": other_copy_commands,
                "env": '\n'.join([f'ENV {k}={v}' for k, v in env_dict.items()])
            }
            build_context_str = prism.constants.DOCKER_IMAGE_BUILD_TEMPLATE.format(
                **kwargs
            )

            # Create Dockerfile
            dockerfile_path = str(Path(tmpdir) / 'Dockerfile')
            with open(dockerfile_path, 'w') as f:
                f.write(build_context_str)
            f.close()

            # Create image
            client.images.build(
                path=tmpdir,
                tag=new_image_name
            )

        # Remove the old image
        if old_img_version is not None:
            client.images.remove(
                image=image_name + f":{old_img_version}",
                force=True
            )

        # Set agent image
        self.image = new_image_name

    def run(self):
        """
        Run the project using the Docker agent
        """
        # Construct command
        full_tb = self.args.full_tb
        log_level = self.args.log_level
        vars = self.args.vars
        context = self.args.context
        modules = self.args.modules
        all_upstream = self.args.all_upstream
        all_downstream = self.args.all_downstream

        # Namespace to string conversion
        full_tb_cmd = "" if not full_tb else "--full-tb"
        log_level_cmd = "" if log_level == "info" else f"--log-level {log_level}"
        vars_cmd = "" if vars is None else " ".join([
            f"{k}={v}" for k, v in vars.items()
        ])
        context_cmd = "" if context == '{}' else f"--context '{context}'"
        modules_cmd = "" if modules is None else "--modules " + " ".join([
            m for m in modules
        ])
        all_upstream_cmd = "" if not all_upstream else "--all-upstream"
        all_downstream_cmd = "" if not all_downstream else "--all-downstream"

        # Full command
        full_cmd = f"prism run {full_tb_cmd} {log_level_cmd} {vars_cmd} {context_cmd} {modules_cmd} {all_upstream_cmd} {all_downstream_cmd}"  # noqa: E501

        # Get the image name
        if self.image is None:
            image_name_base = f"prism-dockeragent-{self.agent_name}"
            img_list = client.images.list()
            for img in img_list:
                if image_name_base in img.tags[0]:
                    self.image = img.tags[0]

        # Run container
        container = client.containers.run(
            self.image,
            command=full_cmd,
            detach=True,
            stdout=True,
            remove=True
        )

        import prism.ui
        # Get the container logs
        container = client.containers.get(container_id=container.id)
        for log in container.logs(stream=True, stdout=True, stderr=True):
            log_str = log.decode('utf-8')
            no_newline = log_str.replace("\n", "")
            if not re.findall(r"^[\-]+$", no_newline):
                prism.logging.DEFAULT_LOGGER.agent(
                    f"{prism.ui.AGENT_GRAY}{self.image}{prism.ui.RESET} | {no_newline}"
                )
        return


if __name__ == "__main__":
    import argparse

    # Project
    binding_clf = Path('/Users/mihirtrivedi/Documents/datastack/git/prism_examples/examples/ml-intermediate/binding_clf')  # noqa: E501
    project = PrismProject(
        project_dir=binding_clf,
        user_context={},
        which='agent'
    )
    project.setup()

    # Docker agent
    args = argparse.Namespace()
    docker_agent = Docker(
        args,
        "docker_test",
        binding_clf,
        project,
        'agents.yml'
    )

    # Create agent
    docker_agent.create_agent(docker_agent.agent_conf)