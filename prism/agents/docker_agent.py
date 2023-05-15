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
from prism.triggers import TriggerManager
import prism.ui

# Standard library imports
import argparse
import docker
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Union
from tempfile import TemporaryDirectory
import shutil
import json


#################
# Docker client #
#################

# For testing
SERVER_URL = os.environ.get("__PRISM_TEST_DOCKER_SERVER_URL__", None)
if SERVER_URL is not None:
    client = docker.from_env(environment={
        "DOCKER_HOST": SERVER_URL
    })
else:
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
        project: PrismProject,
        mode: str = "prod"
    ):
        super().__init__(args, agent_dir, agent_filename, agent_conf, project, mode)

        if mode == "prod":
            # Image name, version
            project_name = self.project.project_dir.name.replace("_", "-")
            self.image_name = f"{project_name}-{self.agent_name}"
            self.image_version: Optional[str] = None

            # Get the server URL, if it exists
            if "server_url" in self.agent_conf.keys():
                server_url = self.agent_conf["server_url"]

                # If the specified server URL is blank, then default to
                # "unix://var/run/docker.sock"
                if server_url == "" or server_url is None:
                    if self.args.which in ["agent-apply", "agent-build"]:
                        prism.logging.fire_console_event(
                            prism.logging.DefaultServerURLEvent()
                        )
                        prism.logging.fire_empty_line_event()
                    server_url = prism.constants.DEFAULT_SERVER_URL
            else:
                if self.args.which in ["agent-apply", "agent-build"]:
                    prism.logging.fire_console_event(
                        prism.logging.DefaultServerURLEvent()
                    )
                    prism.logging.fire_empty_line_event()
                server_url = prism.constants.DEFAULT_SERVER_URL

            # In addition, create a low-level API client. We need this to capture the
            # logs when actually building the image.
            if SERVER_URL is not None:
                self.build_client = docker.APIClient(base_url=SERVER_URL)
            else:
                self.build_client = docker.APIClient(base_url=server_url)

            # Iterate through current images and get all images that resemble our image
            # name.
            img_list = client.images.list()
            img_names = []
            img_versions = []
            for img in img_list:
                if img.tags == []:
                    continue
                elif len(re.findall(
                    r"^" + project_name + r"\-" + self.agent_name + r"\:[0-9\.]+$",  # noqa: E501
                    img.tags[0]
                )) > 0:  # noqa: E501
                    name = img.tags[0].split(":")[0]
                    version = img.tags[0].split(":")[1]
                    img_names.append(name)
                    img_versions.append(version)

            # If more than one image is found, then raise a warning and default to the
            # latest image.
            if len(img_versions) > 1:

                # We need to capture the float and string formats of the image version.
                latest_version_float = max([float(x) for x in img_versions])
                latest_version_str: str
                for v in img_versions:
                    if float(v) == latest_version_float:
                        latest_version_str = v

                # Make sure there is a corresponding image to this version.
                try:
                    _ = img_names[img_versions.index(latest_version_str)]
                except KeyError:
                    raise prism.exceptions.RuntimeException(
                        message=f"could not find image associated with `{latest_version_str}`"  # noqa: E501
                    )

                prism.logging.fire_console_event(
                    prism.logging.MultipleAgentsFound(
                        self.image_name, latest_version_str
                    ),
                    log_level='warn'
                )

                self.image_version = latest_version_str

            # If only one image is found, then we're fine
            elif len(img_versions) == 1:
                self.image_version = img_versions[0]

            # Otherwise, this is the first time we're creating the docker image.
            else:
                self.image_version = None

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
            "server_url": str,
            "env": dict
        }

        return self.check_conf_keys(
            agent_conf,
            required_keys,
            optional_keys
        )

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
        if Path(src).is_dir():
            shutil.copytree(
                src,
                target,
                dirs_exist_ok=True
            )
        elif Path(src).is_file() and not target.is_file():
            # Make the parent directory first
            if not os.path.exists(Path(target).parent):
                os.makedirs(Path(target.parent), exist_ok=True)

            # Copy file
            shutil.copyfile(
                src,
                target,
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
                    for var in ["project_dir", "profiles_dir"]:
                        if adapter_conf[var] not in paths:
                            paths.append(adapter_conf[var])
                if adapter_conf["type"] == "bigquery":
                    paths.append(adapter_conf["creds"])

            # Return
            return paths

        # For any error, just return. If there truly is an error in the project, then
        # it will be revealed when the user runs the project in the agent's computing
        # environment.
        except Exception:
            return paths

    def parse_triggers_path(self,
        project: PrismProject
    ) -> List[Path]:
        """
        Users can augment their sys.path by specifying paths in the `include` key in
        triggers.yml. The paths under this key tell Prism where to look to import the
        trigger functions specified. We will need to copy these over into our agent as
        well.
        """
        # Load the user's triggers YML
        triggers_yml_path = project.triggers_yml_path
        if triggers_yml_path is None:
            return []
        if not Path(triggers_yml_path).is_file():
            return []

        # Create the manager
        try:
            triggers_manager = TriggerManager(
                triggers_yml_path,
                project
            )
            triggers_yml = triggers_manager.load_triggers_yml(triggers_yml_path)
            include_paths = triggers_manager.get_include_paths(triggers_yml)
            return include_paths

        # For any error, just return. If there truly is an error in the project, then
        # it will be revealed when the user runs the project in the agent's computing
        # environment.
        except Exception:
            return []

    def prepare_paths_for_copy(self,
        project: PrismProject,
        tmpdir: str,
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
        profile_yml_path = project.profile_yml_path
        triggers_yml_path = project.triggers_yml_path

        # Get the profile dir
        profile_yml_dir = None if profile_yml_path is None else Path(profile_yml_path).parent  # noqa: E501

        # Get the triggers dir
        triggers_dir = None if triggers_yml_path is None else Path(triggers_yml_path).parent  # noqa: E501

        # Get paths of files / directories associated with profiles
        profile_paths = self.parse_profile_paths(project)

        # Get triggers paths
        triggers_paths = self.parse_triggers_path(project)

        # Copy commands
        copy_commands = {}

        # Copy directories into tmpdir
        for _dir in [profile_yml_dir, triggers_dir] + profile_paths + triggers_paths:
            if _dir is None:
                continue

            # If the directory is equivalent to the project directory, also skip. We
            # automatically copy this into our Docker image.
            elif str(_dir) == str(project_dir):
                continue

            else:
                copy_commands[_dir] = f"COPY {str(_dir)[1:]} ./{str(_dir)[1:]}"
                if self.mode == "prod":
                    print(str(_dir)[1:])
                    self._copy_file_dir(
                        _dir,
                        Path(tmpdir) / str(_dir)[1:]
                    )
        for _dir in sys_path_config:
            if _dir is None:
                continue
            copy_commands[_dir] = f"COPY {str(_dir)[1:]} ./{str(_dir)[1:]}"
            if self.mode == "prod":
                print(str(_dir)[1:])
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
            if agent_conf["image"] is None:
                return prism.constants.DEFAULT_DOCKER_IMAGE
            if not isinstance(agent_conf["image"], str):
                raise prism.exceptions.InvalidAgentsConfException(
                    "`image` is not correctly specified... should be a string"
                )
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
            env_vars: Dict[str, str] = agent_conf["env"]
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

        # Update the version number. If there is no image, then set the first image to
        # be version 1.0
        if self.image_version is None:
            new_img_version = "1.0"
        else:
            new_img_version = str(round(float(self.image_version) + 0.1, 1))

        with TemporaryDirectory(prefix="docker") as tmpdir:
            print(tmpdir)
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
            resp = self.build_client.build(
                path=tmpdir,
                tag=f"{self.image_name}:{new_img_version}",
                rm=True
            )
            for _l in resp:
                _l_str = _l.decode('utf-8').strip('\r\n')
                streams = _l_str.split('\n')
                for stm in streams:
                    if "stream" in stm:
                        log = json.loads(
                            stm.replace('\r', '').replace('\\n', '')
                        )["stream"]
                        if len(re.findall(r'^\s*$', log)) > 0:
                            continue
                        prism.logging.DEFAULT_LOGGER.agent(  # type: ignore
                            f"{prism.ui.AGENT_EVENT}{self.image_name}:{new_img_version}{prism.ui.AGENT_WHICH_BUILD}[build]{prism.ui.RESET} | {log}"  # noqa: E501
                        )

        # Remove the old image
        if self.image_version is not None:
            client.images.remove(
                image=f"{self.image_name}:{self.image_version}",
                force=True
            )
        self.image_version = new_img_version

    def run(self):
        """
        Run the project using the Docker agent
        """
        # Full command
        full_cmd = self.construct_command()

        # Run container
        container = client.containers.run(
            f"{self.image_name}:{self.image_version}",
            command=full_cmd,
            detach=True,
            stdout=True,
            remove=True
        )

        # Get the container logs
        container = client.containers.get(container_id=container.id)
        for log in container.logs(stream=True, stdout=True, stderr=True):
            log_str = log.decode('utf-8')
            no_newline = log_str.replace("\n", "")
            if not re.findall(r"^[\-]+$", no_newline):
                prism.logging.DEFAULT_LOGGER.agent(  # type: ignore
                    f"{prism.ui.AGENT_EVENT}{self.image_name}:{self.image_version}{prism.ui.AGENT_WHICH_RUN}[run]{prism.ui.RESET} | {no_newline}"  # noqa: E501
                )
        return

    def delete(self):
        """
        Delete the Docker agent
        """
