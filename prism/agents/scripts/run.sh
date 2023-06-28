#!/bin/bash

while getopts p:u:n:d:c: flag
do
	case "${flag}" in
		p) pem_path=${OPTARG};;
		u) user=${OPTARG};;
		n) public_dns_name=${OPTARG};;
		d) project_dir=${OPTARG};;
		c) command=${OPTARG};;
	esac
done



# Test the SSH connection
while true
do
  	ssh -o "StrictHostKeyChecking no" -i "${pem_path}" "${user}@${public_dns_name}" exit 2>/dev/null 2>&1
    if [ $? -eq 0 ]; then
        break
    else
        echo "SSH connection failed. Retrying in 5 seconds..."
        sleep 5
    fi
done

# Run the Prism command via SSH
project_name="$(basename -- ${project_dir})"
ssh -i ${pem_path} ${user}@${public_dns_name} "source ~/.venv/${project_name}/bin/activate; cd ../..; cd ${project_dir}; ${command}"
