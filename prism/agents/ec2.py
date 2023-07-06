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
import prism.prism_logging
from prism.triggers import TriggerManager
import prism.ui
from prism.mixins.aws import AwsMixin
import urllib.request

# Standard library imports
import argparse
import boto3
import botocore
from enum import Enum
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Dict, List, Optional
import subprocess


####################
# Class definition #
####################

class State(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    SHUTTING_DOWN = "shutting-down"
    TERMINATED = "terminated"


class IpAddressType(str, Enum):
    V4 = "ipv4"
    V6 = "ipv6"


class Ec2(
    Agent,
    AwsMixin,
):

    def __init__(self,
        args: argparse.Namespace,
        agent_dir: Path,
        agent_filename: str,
        agent_conf: Dict[str, Any],
        project: PrismProject,
        mode: str = "prod"
    ):
        super().__init__(args, agent_dir, agent_filename, agent_conf, project, mode)

        # Bash dir
        self.SCRIPTS_DIR = f"{os.path.dirname(__file__)}/scripts"
        self.AGENT_APPLY_SCRIPT = f"{self.SCRIPTS_DIR}/apply.sh"
        self.AGENT_RUN_SCRIPT = f"{self.SCRIPTS_DIR}/run.sh"

        # Create the client
        self.aws_cli()
        self.ec2_client = boto3.client('ec2')
        self.ec2_resource = boto3.resource('ec2')

        # Instance name
        project_name = Path(self.project.project_dir).name
        self.instance_name = f"{project_name}-{self.agent_name}"

        # Get previous instance data.
        if not Path(prism.constants.INTERNAL_FOLDER / 'ec2.json').is_file():
            self.instance_id: Optional[str] = None
            self.public_dns_name: Optional[str] = None
            self.security_group_id: Optional[str] = None
            self.key_name: Optional[str] = None
            self.pem_key_path: Optional[Path] = None
            self.state: Optional[str] = None
        else:
            with open(Path(prism.constants.INTERNAL_FOLDER / 'ec2.json'), 'r') as f:
                data = json.loads(f.read())
            f.close()

            # If the data exists, then it must be a JSON with two keys: "resources" and
            # "files".
            for attr in [
                "instance_id",
                "public_dns_name",
                "security_group_id",
                "key_name",
                "state"
            ]:
                if attr in data["resources"].keys():
                    self.__setattr__(attr, data["resources"][attr])
                else:
                    self.__setattr__(attr, None)

            # Set PEM key path
            if "pem_key_path" in data["files"].keys():
                self.pem_key_path = Path(data["files"]["pem_key_path"])

    def write_json(self, data: Dict[str, Dict[str, Any]]):
        """
        Write `data` to ~/.prism/ec2.json

        args:
            data: data to write to JSON
        """
        with open(Path(prism.constants.INTERNAL_FOLDER) / 'ec2.json', 'w') as f:
            json.dump(data, f)

    def is_valid_conf(self, agent_conf: Dict[str, Any]):
        """
        A EC2 agent should be formatted as follows:

        agent:
          type: ec2
          pem_key_path: <path to PEM key>
          requirements: <path to requirements.txt>
          python_version: <valid Python build>
          env:
            var1: value1
            var2: value2
        ...
        """
        required_keys = {
            "type": str,
            "instance_type": str,
            "requirements": str,
        }
        optional_keys = {
            "env": dict
        }

        return self.check_conf_keys(
            agent_conf,
            required_keys,
            optional_keys
        )

    def check_resources(self,
        ec2_client: Any,
        instance_name: str,
        instance_id: Optional[str]
    ):
        """
        In order for our agent to work properly, we need three resources:
            - A key-pair
            - A security group
            - An EC2 instance

        All three of these resources should share the same name: `instance_name`. Note
        that multiple EC2 instances can share the same name. That's why we also need
        `instance_id`.

        args:
            ec2_client: Boto3 EC2 client
            instance_name: name of resources
            instance_id: EC2 instance ID
        returns:
            dictionary of resources
        """
        resources: Dict[str, Optional[Dict[str, str]]] = {}

        # Key pair
        resources["key_pair"] = None
        keypairs = ec2_client.describe_key_pairs()
        for kp in keypairs["KeyPairs"]:
            if kp["KeyName"] == instance_name:
                resources["key_pair"] = kp

        # Security groups
        resources["security_group"] = None
        security_groups = ec2_client.describe_security_groups()
        for sg in security_groups["SecurityGroups"]:
            if sg["GroupName"] == instance_name:
                resources["security_group"] = sg

        # Instance
        resources["instance"] = None
        response = ec2_client.describe_instances()
        reservations = response["Reservations"]
        if len(reservations) > 0 and instance_id is not None:
            for res in reservations:
                instances = res["Instances"]
                for inst in instances:
                    if inst["InstanceId"] == instance_id:

                        # Check if key-name and security group for instance matches
                        flag_instance_sg_match = False
                        flag_instance_kp_match = False
                        for sg in inst["SecurityGroups"]:
                            if sg["GroupName"] == instance_name:
                                flag_instance_sg_match = True
                        if inst["KeyName"] == instance_name:
                            flag_instance_kp_match = True

                        # Raise appropriate errors
                        if not flag_instance_sg_match:
                            raise prism.exceptions.AwsException(
                                f"instance {instance_id} does not have security group `{instance_name}`"  # noqa: E501
                            )

                        if not flag_instance_kp_match:
                            raise prism.exceptions.AwsException(
                                f"instance {instance_id} does not have key pair `{instance_name}`"  # noqa: E501
                            )

                        # Otherwise, set to True
                        resources["instance"] = inst

        # Return the resources
        return resources

    def check_instance_data(self,
        ec2_client: Any,
        instance_id: Optional[str]
    ) -> Dict[str, Any]:
        """
        Check if the instance exists

        args:
            ec2_client: Boto3 EC2 client
            instance_id: instance ID
        returns:
            empty dictionary if instance does not exist. otherwise, a dictionary with
            {
                "instance_id": ...,
                "public_dns_name": ...,
                "key_name": ...,
                "state": ...,
            }
        """
        results: Dict[str, Any] = {}

        # If the instance is None, then return an empty dictionary. This happens if the
        # user is creating their first instance or deleted their previous agent and is
        # re-creating one.
        if instance_id is None:
            return results

        # Describe instances and iterate through them
        response = ec2_client.describe_instances()
        reservations = response["Reservations"]
        if len(reservations) > 0:
            for res in reservations:
                instances = res["Instances"]
                for inst in instances:
                    if inst["InstanceId"] == instance_id:
                        results["instance_id"] = instance_id
                        results["public_dns_name"] = inst["PublicDnsName"]
                        results["key_name"] = inst["KeyName"]
                        results["security_groups"] = inst["SecurityGroups"]
                        results["state"] = State(inst["State"]["Name"])

        # Return
        return results

    def restart_instance(self,
        ec2_client,
        state: State,
        instance_id: str,
    ) -> Optional[State]:
        """
        If the instance name already exists, check if it's running. If it isn't (i.e.,
        it's been stopped), then restart it.

        args:
            ec2_client: Boto3 EC2 client
            state: state of instance
            instance_name: name of instance to restart
            instance_id: ID of instance to restart
        returns:
            State of restarted instance (should only be State.RUNNING)
        """
        if state is None:
            raise prism.exceptions.RuntimeException(
                "`start_stopped_instance` called on a nonexistent instance!"  # noqa: E501
            )

        # If the instance is pending, then wait until the state is running
        elif state == State.PENDING:
            while state == State.PENDING:
                results = self.check_instance_data(ec2_client, instance_id)
                state = results["state"]
                time.sleep(1)
            return state

        # If the instance is stopping / stopped, then restart it and wait until the
        # state is `running`.
        elif state in [State.STOPPED, State.STOPPING]:
            ec2_client.start_instances(InstanceIds=instance_id)
            while state != State.RUNNING:
                results = self.check_instance_data(ec2_client, instance_id)
                time.sleep(1)
                state = results["state"]
            return state

        # If nothing's been returned, then the instance should already be running
        return state

    def check_ip_address_type(self,
        external_ip: str
    ) -> IpAddressType:
        """
        Determine whether `external_ip` is an IPv4 or IPv6 address

        args:
            external_ip: external IP address
        returns:
            IpAddressType
        """
        flag_is_ipv4 = re.findall(
            r'(^(?:\d{1,3}\.){3}\d{1,3}$)',
            external_ip
        )
        flag_is_ipv6 = re.findall(
            r'^((?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4})$',
            external_ip
        )

        # IP address must be IPv4 or IPv6, but not both
        if flag_is_ipv4 and flag_is_ipv6:
            raise prism.exceptions.RuntimeException(
                f"Unrecognized IP address type `{external_ip}`"
            )
        if not (flag_is_ipv4 or flag_is_ipv6):
            raise prism.exceptions.RuntimeException(
                f"Unrecognized IP address type `{external_ip}`"
            )

        # Return
        if flag_is_ipv4:
            return IpAddressType('ipv4')
        else:
            return IpAddressType('ipv6')

    def add_ingress_rule(self,
        ec2_client: Any,
        security_group_id: str,
        external_ip: str
    ):
        """
        Add an ingress rule that allows SSH traffic from `external_ip`

        args:
            ec2_client: Boto3 EC2 client
            security_group_id: security group ID
            external_ip: external IP address from which to allow traffic
        returns:
            None
        """
        ip_address_type = self.check_ip_address_type(external_ip)

        # Add rule
        if ip_address_type == IpAddressType('ipv4'):
            ip_permissions = [
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 22,
                    'ToPort': 22,
                    'IpRanges': [{'CidrIp': f'{external_ip}/32'}]
                },
            ]
        else:
            ip_permissions = [
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 22,
                    'ToPort': 22,
                    'Ipv6Ranges': [{'CidrIpv6': f'{external_ip}'}]
                },
            ]

        _ = ec2_client.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=ip_permissions
        )

    def create_new_security_group(self,
        ec2_client: Any,
        instance_name: str,
    ):
        """
        Create a new security group for our EC2 instance. This security group allows
        traffic from the user's IP only.

        args:
            ec2_client: Boto3 EC2 client
            instance_id: instance ID
        returns:
            newly created security group ID
        """

        # Default VPC
        response = ec2_client.describe_vpcs()
        vpc_id = response.get('Vpcs', [{}])[0].get('VpcId', '')

        # Create the security group
        response = ec2_client.create_security_group(
            GroupName=instance_name,
            Description=f'VPC for {instance_name} EC2 agent',
            VpcId=vpc_id
        )
        security_group_id = response['GroupId']

        # IP address
        external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')
        self.add_ingress_rule(
            ec2_client,
            security_group_id,
            external_ip
        )
        return security_group_id, vpc_id

    def check_ingress_ip(self,
        ec2_client: Any,
        security_group_id: str
    ):
        """
        Confirm that the ingress rule for `security_group_id` allows for SSH traffic
        from the user's IP address.
        """
        # Get security group
        security_groups = ec2_client.describe_security_groups()["SecurityGroups"]
        curr_sg = None
        for sg in security_groups:
            if sg["GroupId"] == security_group_id:
                curr_sg = sg
        if curr_sg is None:
            raise prism.exceptions.AwsException(
                f"could not find security group with ID `{security_group_id}`"
            )

        # Get current IP address
        external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')
        external_ip_type = self.check_ip_address_type(external_ip)

        # Check if IP is in ingress rules
        for ingress_permissions in curr_sg["IpPermissions"]:

            # Look at SSH protocols only
            if (
                ingress_permissions["FromPort"] == 22
                and ingress_permissions["IpProtocol"] == "tcp"  # noqa: W503
                and ingress_permissions["ToPort"] == 22  # noqa: W503
            ):
                # Check if SSH traffic from the current IP address is allowed
                ip_allowed = False
                if external_ip_type == IpAddressType('ipv4'):
                    ip_ranges = ingress_permissions["IpRanges"]
                    for ipr in ip_ranges:
                        if external_ip in ipr["CidrIp"]:
                            ip_allowed = True
                else:
                    ip_ranges = ingress_permissions["Ipv6Ranges"]
                    for ipr in ip_ranges:
                        if external_ip in ipr["CidrIpv6"]:
                            ip_allowed = True

        # If SSH traffic from the current IP address it not allowed, then authorize it.
        if not ip_allowed:
            self.add_ingress_rule(
                ec2_client,
                security_group_id,
                external_ip
            )

    def create_instance(self,
        ec2_client: Any,
        ec2_resource: Any,
        instance_id: Optional[str],
        instance_name: str,
        instance_type: str,
    ):
        """
        Create EC2 instance

        args:
            ec2_client: Boto3 AWS EC2 client
            ec2_resource: Boto3 AWS EC2 resource
            instance_id: EC2 instance ID
            instance_name: name of EC2 instance
            instance_type: EC2 instance types
        returns:
            EC2 response
        """
        # Data to write
        data = {}

        # Check resources
        resources = self.check_resources(ec2_client, instance_name, instance_id)

        # Log prefix
        log_prefix = f"{prism.ui.AGENT_EVENT}{instance_name}{prism.ui.AGENT_WHICH_BUILD}[build]{prism.ui.RESET}"  # noqa: E501

        def _create_exception(resource):
            return prism.exceptions.AwsException('\n'.join([
                f"{resource} exists, but ~/.prism/ec2.json file not found! This only happens if:",  # noqa: E501
                f"    1. You manually created the {resource}",
                "    2. You deleted ~/.prism/ec2.json",
                f"Delete the {resource} from EC2 and try again!"
            ]))

        # Create PEM key pair
        if resources["key_pair"] is None:
            pem_key_path = self.create_key_pair(
                ec2_client,
                key_name=instance_name,
            )
            log_instance_name = f"{prism.ui.MAGENTA}{instance_name}{prism.ui.RESET}"
            prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                f"{log_prefix} | Created key pair {log_instance_name}"
            )
        else:
            # If the key-pair exists, then the location of the PEM key path should be
            # contained in ~/.prism/ec2.json. If it isn't, then either:
            #   1. The user manually created the key pair
            #   2. The user deleted ~/.prism/ec2.json
            if not Path(prism.constants.INTERNAL_FOLDER / 'ec2.json').is_file():
                raise _create_exception("key-pair")
            pem_key_path = self.pem_key_path

            # Log
            log_instance_name = f"{prism.ui.MAGENTA}{instance_name}{prism.ui.RESET}"  # noqa: E501
            log_instance_path = f"{prism.ui.MAGENTA}{str(pem_key_path)}{prism.ui.RESET}"  # noqa: E501
            prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                f"{log_prefix} | Using existing key-pair {log_instance_name} at {log_instance_path}"  # noqa: E501
            )

        # Security group
        if resources["security_group"] is None:
            security_group_id, vpc_id = self.create_new_security_group(
                ec2_client,
                instance_name
            )

            # Log
            log_security_group_id = f"{prism.ui.MAGENTA}{security_group_id}{prism.ui.RESET}"  # noqa: E501
            log_vpc_id = f"{prism.ui.MAGENTA}{vpc_id}{prism.ui.RESET}"  # noqa: E501
            prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                f"{log_prefix} | Created security group with ID {log_security_group_id} in VPC {log_vpc_id}"  # noqa: E501
            )
        else:
            if not Path(prism.constants.INTERNAL_FOLDER / 'ec2.json').is_file():
                raise _create_exception("security group")

            # Log
            security_group_id = self.security_group_id
            self.check_ingress_ip(ec2_client, security_group_id)
            log_security_group_id = f"{prism.ui.MAGENTA}{security_group_id}{prism.ui.RESET}"  # noqa: E501
            prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                f"{log_prefix} | Using existing security group {log_security_group_id}"
            )

        # Log instance ID template
        log_instance_id_template = f"{prism.ui.MAGENTA}{{instance_id}}{prism.ui.RESET}"

        # Instance
        if resources["instance"] is None:
            instance = ec2_resource.create_instances(
                InstanceType=instance_type,
                KeyName=instance_name,
                MinCount=1,
                MaxCount=1,
                ImageId="ami-0889a44b331db0194",
                TagSpecifications=[
                    {
                        'ResourceType': 'instance',
                        'Tags': [
                            {
                                'Key': 'Name',
                                'Value': instance_name
                            },
                        ]
                    },
                ],
                SecurityGroupIds=[
                    security_group_id
                ]
            )
            instance_id = instance[0].id

            # Log
            prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                f"{log_prefix} | Created EC2 instance with ID {log_instance_id_template.format(instance_id=instance_id)}"  # noqa: E501
            )
            time.sleep(1)
        else:
            if not Path(prism.constants.INTERNAL_FOLDER / 'ec2.json').is_file():
                raise _create_exception("instance")
            instance_id = self.instance_id

            # Log
            prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                f"{log_prefix} | Using existing EC2 instance with ID {log_instance_id_template.format(instance_id=instance_id)}"  # noqa: E501
            )

        # Instance data
        resp = self.check_instance_data(ec2_client, instance_id)

        # If the instance exists but its key-name is not `instance_name` (this really
        # should never happen unless the user manually creates an EC2 instance that has
        # the same name), then raise an error
        if len(resp.keys()) > 0 and resp["key_name"] != instance_name:
            raise prism.exceptions.AwsException(
                f"unrecognized key `{resp['key_name']}`...the agent requires key `{instance_name}.pem`"  # noqa: E501
            )

        # If the instance exists and is running, then just return
        elif len(resp.keys()) > 0 and resp["state"] in [State.PENDING, State.RUNNING]:
            while resp["state"] == State.PENDING:

                # Log
                log_pending_status = f"{prism.ui.YELLOW}pending{prism.ui.RESET}"
                prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                    f"{log_prefix} | Instance {log_instance_id_template.format(instance_id=instance_id)} is `{log_pending_status}`... checking again in 5 seconds"  # noqa: E501
                )
                resp = self.check_instance_data(
                    ec2_client,
                    instance_id
                )
                time.sleep(5)

        # If the state exiss but has stopped, then restart it
        elif len(resp.keys()) > 0 and resp["state"] in [State.STOPPED, State.STOPPING]:
            self.restart_instance(
                ec2_client,
                resp["state"],
                resp["instance_id"]
            )

        # Write data
        data = {
            "resources": {
                "instance_id": instance_id,
                "public_dns_name": resp["public_dns_name"],
                "security_group_id": security_group_id,
                "key_name": instance_name,
                "state": resp["state"]
            },
            "files": {
                "pem_key_path": str(pem_key_path)
            }
        }
        self.write_json(data)

        # Update class attributes
        self.instance_id = instance_id
        self.public_dns_name = resp["public_dns_name"]
        self.security_group_id = security_group_id
        self.key_name = instance_name
        self.state = resp["state"]
        self.pem_key_path = pem_key_path

        # Return the data
        return data

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

    def get_all_project_paths(self,
        project: PrismProject,
    ):
        """
        Prism projects often rely on more than just their own directory. They can import
        functions / tasks or reference configuration files from other directories. We
        need to copy all relevant directories into our agent, and we need to ensure that
        these paths have the same relative location to the project directory.

        args:
            project: Prism project
        returns:
            list of project paths
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

        # Copy directories into tmpdir

        unique_paths = list(
            set(
                sys_path_config
                + [profile_yml_dir, triggers_dir]  # noqa: W503
                + profile_paths  # noqa: W503
                + triggers_paths  # noqa: W503
            )
        )

        return [str(x) for x in unique_paths if str(x) != str(project_dir)]

    def parse_instance_type(self,
        agent_conf: Dict[str, Any]
    ) -> Any:
        """
        Get the user-specified instance type from the agent's configuration

        args:
            agent_conf: agent configuration as dictionary
        returns:
            `pip install` command for requirements
        """
        return agent_conf["instance_type"]

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

    def _log_output(self,
        color: str,
        which: str,
        output: Any,
    ):
        if output:
            if isinstance(output, str):
                if not re.findall(r"^[\-]+$", output.rstrip()):
                    prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                        f"{prism.ui.AGENT_EVENT}{self.instance_name}{color}[{which}]{prism.ui.RESET} | {output.rstrip()}"  # noqa: E501
                    )
            else:
                if not re.findall(r"^[\-]+$", output.decode().rstrip()):
                    prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                        f"{prism.ui.AGENT_EVENT}{self.instance_name}{color}[{which}]{prism.ui.RESET} | {output.decode().rstrip()}"  # noqa: E501
                    )

    def stream_logs(self,
        cmd: List[str],
        color: str,
        which: str
    ):
        """
        Stream Bash script logs. We use bash scripts to run our `apply` and `run`
        commands.

        args:
            cmd: subprocess command
            color: color to use in log styling
            which: one of `build` or `run`
        returns:
            subprocess return code
        """
        # Open a subprocess and stream the logs
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            universal_newlines=True,
        )
        while True:
            # For whatever reason, the `prism` command places the log in stderr, not
            # stdout
            if which == "build":
                output = process.stdout.readline()  # type: ignore
            else:
                output = process.stderr.readline()  # type: ignore

            # Stream the logs
            if process.poll() is not None:
                break
            self._log_output(color, which, output)

        return process.stdout, process.stderr, process.returncode

    def apply(self):
        """
        Create the EC2 instance image
        """
        # Fire an empty line -- it just looks a little nicer
        prism.prism_logging.fire_console_event(prism.prism_logging.EmptyLineEvent())

        # Instance type
        instance_type = self.parse_instance_type(self.agent_conf)

        # requirements.txt path
        requirements_txt_path = Path(self.parse_requirements(self.agent_conf))

        # Environment dictionary
        env_dict = self.parse_environment_variables(self.agent_conf)
        env_cli = ",".join([f"{k}={v}" for k, v in env_dict.items()])

        # Paths to copy
        project_paths = self.get_all_project_paths(self.project)
        project_paths_cli = ",".join(project_paths)

        # Create the instance
        data = self.create_instance(
            self.ec2_client,
            self.ec2_resource,
            self.instance_id,
            self.instance_name,
            instance_type
        )

        # The `create_instance` command is blocking — it won't finish until the instance
        # is up and running.
        user = "ec2-user"
        public_dns_name = data["resources"]["public_dns_name"]
        pem_key_path = data["files"]["pem_key_path"]

        # Build the shell command
        cmd = [
            '/bin/sh', self.AGENT_APPLY_SCRIPT,
            '-r', str(requirements_txt_path),
            '-p', str(pem_key_path),
            '-u', user,
            '-n', public_dns_name,
            '-d', str(self.project.project_dir),
            '-c', project_paths_cli,
            '-e', env_cli,
        ]

        # Open a subprocess and stream the logs
        _, err, returncode = self.stream_logs(cmd, prism.ui.AGENT_WHICH_BUILD, "build")

        # Log anything from stderr that was printed in the project
        for line in err.readlines():
            prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                f"{prism.ui.AGENT_EVENT}{self.instance_name}{prism.ui.AGENT_WHICH_BUILD}[build]{prism.ui.RESET} | {line.rstrip()}"  # noqa: E501
            )

        # Return the returncode. Return a dictionary in order to avoid confusing this
        # output with the output of an event manager.
        return {"return_code": returncode}

    def run(self):
        """
        Run the project using the EC2 agent
        """
        # Full command
        full_cmd = self.construct_command()

        # Logging styling
        if self.instance_name is None or self.instance_id is None:
            prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                "Agent data not found! Use `prism agent apply` to create your agent"
            )
            return

        # Check the ingress rules
        if self.security_group_id is not None:
            self.check_ingress_ip(self.ec2_client, self.security_group_id)

        # The agent data should exist...Build the shell command
        cmd = [
            '/bin/sh', self.AGENT_RUN_SCRIPT,
            '-p', str(self.pem_key_path),
            '-u', 'ec2-user',
            '-n', self.public_dns_name,
            '-d', str(self.project.project_dir),
            '-c', full_cmd,
        ]
        out, _, returncode = self.stream_logs(cmd, prism.ui.AGENT_WHICH_RUN, "run")

        # Log anything from stdout that was printed in the project
        for line in out.readlines():
            prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                f"{prism.ui.AGENT_EVENT}{self.instance_name}{prism.ui.AGENT_WHICH_RUN}[run]{prism.ui.RESET} | {line.rstrip()}"  # noqa: E501
            )

        # Return the returncode. Return a dictionary in order to avoid confusing this
        # output with the output of an event manager.
        return {"return_code": returncode}

    def delete(self):
        """
        Delete all resources associated with agent. This includes:
            - Key pair
            - Security group
            - Instance

        In addition, remove the PEM key from our local files
        """
        # Fire an empty line -- it just looks a little nicer
        prism.prism_logging.fire_console_event(prism.prism_logging.EmptyLineEvent())

        # Logging styling
        if self.instance_name is None:
            prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                "Agent data not found! Did you manually delete the ~/.prism/ec2.json file?"  # noqa: E501
            )
            return

        # Logging styling
        log_prefix = f"{prism.ui.AGENT_EVENT}{self.instance_name}{prism.ui.RED}[delete]{prism.ui.RESET}"  # noqa: E501

        # Key pair
        if self.key_name is None:
            prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                f"{log_prefix} | No agent data found!"
            )
        else:
            log_key_pair = f"{prism.ui.MAGENTA}{self.key_name}{prism.ui.RESET}"
            log_key_path = f"{prism.ui.MAGENTA}{str(self.pem_key_path)}{prism.ui.RESET}"
            prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                f"{log_prefix} | Deleting key-pair {log_key_pair} at {log_key_path}"
            )
            self.ec2_client.delete_key_pair(
                KeyName=self.key_name
            )
            os.unlink(str(self.pem_key_path))

        # Instance
        if self.instance_id is None:
            prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                f"{log_prefix} | No instance found!"
            )
        else:
            log_instance_id = f"{prism.ui.MAGENTA}{self.instance_id}{prism.ui.RESET}"
            prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                f"{log_prefix} | Deleting instance {log_instance_id}"
            )
            _ = self.ec2_client.terminate_instances(
                InstanceIds=[self.instance_id]
            )

        # Security group
        if self.security_group_id is None:
            prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                f"{log_prefix} | No security group found! If this is a mistake, then you may need to reset your resource data"  # noqa: E501
            )
        else:
            log_security_group_id = f"{prism.ui.MAGENTA}{self.security_group_id}{prism.ui.RESET}"  # noqa: E501
            while True:
                try:
                    self.ec2_client.delete_security_group(
                        GroupId=self.security_group_id
                    )
                    prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                        f"{log_prefix} | Deleting security group {log_security_group_id}"  # noqa: E501
                    )
                    break
                except botocore.exceptions.ClientError as e:
                    if "DependencyViolation" in str(e):
                        prism.prism_logging.DEFAULT_LOGGER.agent(  # type: ignore
                            f"{log_prefix} | Encountered `DependencyViolation` when deleting security group {log_security_group_id}...waiting 5 seconds and trying again"  # noqa: E501
                        )
                        time.sleep(5)
                    else:
                        raise e

        # Remove the data
        if Path(prism.constants.INTERNAL_FOLDER / 'ec2.json').is_file():
            os.unlink(Path(prism.constants.INTERNAL_FOLDER / 'ec2.json'))
