#!/bin/bash
# By --> Pravin Bhagade

# Ask if it's Ambari or Cloudera Manager
read -p "Is this an Ambari Server or Cloudera Manager? (Enter 'Ambari' or 'Cloudera'): " SERVER_TYPE

# Convert the input to lowercase
SERVER_TYPE=$(echo "$SERVER_TYPE" | tr '[:upper:]' '[:lower:]')

# Ask the user for the Server's IP address
read -p "Enter the Server's IP address: " SERVER_IP

# Set the root password
ROOT_PASSWORD="Passw0rd"
SSH_USER="root"
CURRENT_IP=$(hostname -I | awk '{print $1}')
CURRENT_HOSTNAME=$(hostname)
export USER=admin
export PASSWORD=admin

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

if [ "$SERVER_TYPE" == "ambari" ]; then
    export PORT=8080
    export PROTOCOL=http
    output=$(curl -ksu $USER:$PASSWORD -i -H 'X-Requested-By: ambari' $PROTOCOL://$SERVER_IP:$PORT/api/v1/clusters)
    CLUSTER=$(echo "$output" | sed -n 's/.*"cluster_name" : "\([^\"]*\)".*/\1/p')
    HOSTS=$(curl -su $USER:$PASSWORD $PROTOCOL://$SERVER_IP:$PORT/api/v1/clusters/$CLUSTER/hosts | grep -o '"host_name" : "[^"]*' | awk -F'"' '{print $4}')
elif [ "$SERVER_TYPE" == "cloudera" ]; then
    export PORT=7180
    API_VERSION=$(curl -s -u $USER:$PASSWORD "http://$SERVER_IP:$PORT/api/version")
    HOSTS=$(curl -su $USER:$PASSWORD "http://$SERVER_IP:$PORT/api/$API_VERSION/hosts" | grep -oP '(?<="hostname" : ")[^"]*')
else
    echo "Invalid input. Please enter 'Ambari' or 'Cloudera'."
    exit 1
fi

# Update /etc/hosts with host entries
for host in $HOSTS; do
    if [ "$SERVER_TYPE" == "ambari" ]; then
        ip=$(curl -su $USER:$PASSWORD "$PROTOCOL://$SERVER_IP:$PORT/api/v1/clusters/$CLUSTER/hosts/$host" | grep -o '"ip" : "[^"]*' | awk -F'"' '{print $4}')
    else
        ip=$(curl -su $USER:$PASSWORD "http://$SERVER_IP:$PORT/api/$API_VERSION/hosts" | grep -oP '(?<="ipAddress" : ")[^"]*')
    fi

    if ! grep -q "$host" /etc/hosts; then
        echo "$ip $host" | sudo tee -a /etc/hosts
        echo "Added $host to /etc/hosts"
    fi
done

# SSH loop to append to /etc/hosts on each host
for host in $HOSTS; do
    sshpass -p "$ROOT_PASSWORD" ssh -o StrictHostKeyChecking=no $SSH_USER@$host "echo '$CURRENT_IP $CURRENT_HOSTNAME' | sudo tee -a /etc/hosts"
done

# Generate an SSH key pair if it doesn't exist
filename="id_rsa"
path="$HOME/.ssh"

# Check if the RSA private key file already exists
if [ -f "$path/$filename" ]; then
    echo "RSA key exists at $path/$filename, using the existing file."
else
    # Generate RSA key pair without prompts
    ssh-keygen -t rsa -N '' -f "$path/$filename"
    echo "RSA key pair generated."
fi

# Collect host names
HOST_NAMES=$HOSTS

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

echo "Passwordless SSH setup completed for all nodes and all added Pulse hostname to all cluster nodes"
