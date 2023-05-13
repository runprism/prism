"""
Mixin class utilizing the AWS Python SDK.

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

import os
from pathlib import Path
import shutil
from typing import Any
import stat

import prism.constants
import prism.exceptions


####################
# Class definition #
####################

class AwsMixin():
    """
    Mixin class utilizing the AWS Python SDK. This is used for creating AWS agents.
    """

    def aws_cli(self) -> int:
        """
        Confirms that the user has configured their AWS CLI
        args:
            None
        returns:
            0 if user has configured AWS CLI
        raises:
            pipe.exceptions.RuntimeException() if user has not configured AWS CLI
        """
        system_return = shutil.which('aws')
        if system_return is None:
            msg_list = [
                "AWS CLI is not properly configured. Consult AWS documentation:",
                "https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html"    # noqa: E501
            ]
            raise prism.exceptions.RuntimeException(message='\n'.join(msg_list))
        return 0

    def create_key_pair(self,
        ec2_client: Any,
        key_name: str,
        directory: Path = Path(os.path.expanduser("~/.aws"))
    ) -> Path:
        """
        Create a PEM key pair. This PEM key is required to SSH / copy files into our EC2
        instance / EMR cluster. We will call this function before the user first creates
        their instance.

        args:
            client: Boto3 EC2 client key_name: name of the new key pair directory:
            directory in which to place the keypair; default is ~/.aws/
        returns:
            path to newly created PEM key
        raises:
            UnauthorizedOperation if the user does not have the required permissions to
            create a key pair
        """
        response = ec2_client.create_key_pair(
            KeyName=key_name,
            KeyType="rsa",
            KeyFormat="pem"
        )
        if not Path(directory).is_dir():
            Path(directory).mkdir(parents=True)
        with open(Path(directory / f"{key_name}.pem"), 'w') as f:
            f.write(response["KeyMaterial"])

        # Change the permissions
        os.chmod(Path(directory / f"{key_name}.pem"), stat.S_IREAD)

        # We'll need to persist the location of the PEM key across runs. For example,
        # let's say a user calls `agent apply` and creates the key-pair and EC2
        # instance. When they call `agent run`, we will need to use the PEM key created
        # by `agent apply` to execute the operation. For now, return the path. We'll
        # save out a JSON with this path in the agent class.
        return Path(directory / f"{key_name}.pem")

    def delete_key_pair(self,
        ec2_client: Any,
        key_name: str,
        directory: Path = Path(os.path.expanduser("~/.aws"))
    ) -> Path:
        """
        Create a PEM key pair. This PEM key is required to SSH / copy files into our EC2
        instance / EMR cluster.

        args:
            client: Boto3 EC2 client
            key_name: name of the new key pair
            directory: directory in which to place the keypair; default is ~/.aws/
        returns:
            path to newly created PEM key
        raises:
            UnauthorizedOperation if the user does not have the required permissions to
            create a key pair
        """
        _ = ec2_client.delete_key_pair(
            KeyName=key_name,
        )
        if Path(directory / f"{key_name}.pem").is_file():
            os.unlink(Path(directory / f"{key_name}.pem"))
