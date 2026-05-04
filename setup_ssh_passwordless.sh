#!/bin/bash

# Set the root password
ROOT_PASSWORD="Passw0rd"
export AMBARISERVER=$(hostname -f)
export USER=admin
export PASSWORD=admin
export PORT=8080
export PROTOCOL=http
output=$(curl -ksu $USER:$PASSWORD -i -H 'X-Requested-By: ambari' $PROTOCOL://$AMBARISERVER:$PORT/api/v1/clusters)
CLUSTER=$(echo "$output" | sed -n 's/.*"cluster_name" : "\([^\"]*\)".*/\1/p')

# Generate an SSH key pair if it doesn't exist
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -b 4096
fi

# Check if sshpass is installed, and install if missing
if ! command -v sshpass > /dev/null; then
    echo "sshpass is not installed. Installing it..."
    if command -v apt-get > /dev/null; then
        sudo apt-get update
        sudo apt-get install -y sshpass
    elif command -v yum > /dev/null; then
        sudo yum install -y sshpass
    else
        echo "Unsupported package manager. Please install sshpass manually."
        exit 1
    fi
fi

# Collect host names
HOST_NAMES=$(curl -su admin:admin $PROTOCOL://$AMBARISERVER:$PORT/api/v1/clusters/$CLUSTER/hosts | grep -o '"host_name" : "[^"]*' | awk -F'"' '{print $4}')

# Iterate through host names
for host in $HOST_NAMES; do
    # Copy public key to remote machine
    sshpass -p "$ROOT_PASSWORD" ssh-copy-id -o StrictHostKeyChecking=no root@"$host"
    if [ $? -eq 0 ]; then
        echo "Public key copied to $host successfully."
    else
        echo "Failed to copy public key to $host."
    fi
done

echo "Passwordless SSH setup completed for all nodes."
