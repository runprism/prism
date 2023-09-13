#!/bin/bash

while getopts r:p:u:n:d:c:e: flag
do
	case "${flag}" in
		r) requirements=${OPTARG};;
		p) pem_path=${OPTARG};;
		u) user=${OPTARG};;
		n) public_dns_name=${OPTARG};;
		d) project_dir=${OPTARG};;
		c) copy_paths=${OPTARG};;
		e) env=${OPTARG};;
	esac
done

# ssh into the project so that we can authenticate our key
while true
do
  	errormessage=`ssh -o "StrictHostKeyChecking no" -i "${pem_path}" "${user}@${public_dns_name}" exit 2>/dev/null 2>&1`
    if [ -z "$errormessage" ]; then
        echo "SSH connection succeeded!"
        break
    else
		if [[ "$errormessage" =~ "Operation timed out" ]]; then
			echo "SSH connection failed."
			exit 8
		else
        	echo "SSH connection refused. Retrying in 5 seconds..."
        	sleep 5
		fi
    fi
done

# get project name and path as it would appear in cluster
project_name="$(basename -- ${project_dir})"

# Compare local requirements to remote requirements. If the two are identical, then do
# not re-install the requirements. If they aren't, then create a new virtual environment
# and reinstall.
local_file="${requirements}"
remote_file="./requirements.txt"
temp_file=$(mktemp)
scp -i ${pem_path} ${user}@${public_dns_name}:${remote_file} ${temp_file} 2> scp.log
if diff $local_file $temp_file >/dev/null ; then
    true # pass
else
	rm ${temp_file}

	# Copy the local requirements onto the EC2 instance
	scp -i ${pem_path} ${local_file} ${user}@${public_dns_name}:${remote_file} 2> scp.log
	ssh -i ${pem_path} ${user}@${public_dns_name} <<EOF
if [ -d ~/.venv/${project_name} ]; then
	sudo rm -rf ~/.venv/${project_name}
fi
python3 -m venv ~/.venv/${project_name}
source ~/.venv/${project_name}/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
EOF
	exit_code=$?
	if [ $exit_code -eq 1 ]; then
		exit 1
	fi
fi

# Log
echo "Updating remote project and file paths"

# Copy project directory and other copy paths into the EC2 instance
ssh -i ${pem_path} ${user}@${public_dns_name} "sudo mkdir -p .${project_dir}; sudo chmod 777 -R .${project_dir}"
exit_code=$?
if [ $exit_code -eq 1 ]; then
	exit 1
fi
scp -r -i ${pem_path} ${project_dir} ${user}@${public_dns_name}:.${project_dir}
echo "Copied project directory into instance"

IFS=',' read -ra array <<< "${copy_paths}"
for path in "${array[@]}"; do
	# Make a directory and change the permissions
	ssh -i ${pem_path} ${user}@${public_dns_name} "sudo mkdir -p .${path%/*}; sudo chmod 777 -R .${path%/*}"
	exit_code=$?
	if [ $exit_code -eq 1 ]; then
		exit 1
	fi

	# Copy
	scp -r -i ${pem_path} ${path} ${user}@${public_dns_name}:.${path%/*} 2> scp.log
	echo "Copied path ${path} into instance"
done

# Environment variables. Environment variable are passed a comma-separated list of
# key-value pairs, i.e. ENV1=value1,ENV2=value2,...
IFS=',' read -ra env_array <<< "${env}"; unset IFS;
SED_COMMAND=""
for keyvalue in "${env_array[@]}"; do
	IFS='=' read -r key value <<< "${keyvalue}"

	# Update the key-value pair in .bashrc if it exists
    if ssh -i ${pem_path} ${user}@${public_dns_name} "grep -q '^export ${key}=' ~/.bashrc"; then
        ssh -i ${pem_path} ${user}@${public_dns_name} "sed -i 's/^export ${key}=.*$/export ${key}=${value}/' ~/.bashrc"
		exit_code=$?
		if [ $exit_code -eq 1 ]; then
			exit 1
		fi

    # Add the new key-value pair to the end of .bashrc if it doesn't exist
    else
        ssh -i ${pem_path} ${user}@${public_dns_name} "echo 'export ${key}=${value}' >> ~/.bashrc"
		exit_code=$?
		if [ $exit_code -eq 1 ]; then
			exit 1
		fi
    fi
	echo "Updated environment variable ${key}=${value}"
done

# Reload .bashrc to update environment variables
ssh -i ${pem_path} ${user}@${public_dns_name} "source ~/.bashrc"
exit_code=$?
if [ $exit_code -eq 1 ]; then
	exit 1
fi

# Move all folders into the root folder
ssh -i ${pem_path} ${user}@${public_dns_name} 'cd ~ && for dir in */; do sudo rm -rf ../../$dir; sudo mv -f $dir ../../ ; done'
exit_code=$?
if [ $exit_code -eq 1 ]; then
	exit 1
fi
echo "Done updating remote project and file paths"
