#!/bin/bash

# Set the root password
ROOT_PASSWORD="Passw0rd"
export AMBARISERVER=$(hostname -f)
export USER=admin
export PASSWORD=admin
export PORT=8080
export PROTOCOL=http

# Optional bootstrap user/key for hosts where sshd has PasswordAuthentication
# disabled (e.g. cloud-init hardened images). The user must have passwordless
# sudo on the target hosts.
#   ./setup_ssh_passwordless.sh [-u <bootstrap_user>] [-k <bootstrap_key>]
BOOTSTRAP_USER=""
BOOTSTRAP_KEY=""
while getopts "u:k:" opt; do
    case $opt in
        u) BOOTSTRAP_USER="$OPTARG" ;;
        k) BOOTSTRAP_KEY="$OPTARG" ;;
        *) echo "Usage: $0 [-u bootstrap_user] [-k bootstrap_key]"; exit 1 ;;
    esac
done

output=$(curl -ksu $USER:$PASSWORD -i -H 'X-Requested-By: ambari' $PROTOCOL://$AMBARISERVER:$PORT/api/v1/clusters)
CLUSTER=$(echo "$output" | sed -n 's/.*"cluster_name" : "\([^\"]*\)".*/\1/p')

# Generate an SSH key pair if it doesn't exist
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -b 4096 -N '' -f ~/.ssh/id_rsa
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

# Returns 0 if key-based root login to the host already works
root_key_works() {
    ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10 root@"$1" true 2>/dev/null
}

# Installs our public key into root's authorized_keys via the bootstrap
# user + key and sudo, for hosts where root password auth is not offered.
bootstrap_root_key() {
    local host=$1
    local pubkey
    pubkey=$(cat ~/.ssh/id_rsa.pub)
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -i "$BOOTSTRAP_KEY" "$BOOTSTRAP_USER"@"$host" \
        "sudo mkdir -p /root/.ssh && sudo chmod 700 /root/.ssh && \
         sudo grep -qxF '$pubkey' /root/.ssh/authorized_keys 2>/dev/null || \
         echo '$pubkey' | sudo tee -a /root/.ssh/authorized_keys > /dev/null; \
         sudo chmod 600 /root/.ssh/authorized_keys"
}

FAILED_HOSTS=""

# Iterate through host names
for host in $HOST_NAMES; do
    if root_key_works "$host"; then
        echo "Key-based root SSH to $host already works, skipping."
        continue
    fi

    # Copy public key to remote machine (needs sshd PasswordAuthentication yes)
    sshpass -p "$ROOT_PASSWORD" ssh-copy-id -o StrictHostKeyChecking=no root@"$host"
    if [ $? -eq 0 ]; then
        echo "Public key copied to $host successfully."
        continue
    fi

    # Password auth is disabled on hardened images; fall back to installing
    # the key through the bootstrap user's sudo access.
    if [ -n "$BOOTSTRAP_USER" ] && [ -n "$BOOTSTRAP_KEY" ]; then
        echo "Password auth failed for $host, bootstrapping root key via $BOOTSTRAP_USER..."
        bootstrap_root_key "$host"
    fi

    if root_key_works "$host"; then
        echo "Public key installed on $host via bootstrap user."
    else
        echo "Failed to copy public key to $host."
        FAILED_HOSTS="$FAILED_HOSTS $host"
    fi
done

if [ -n "$FAILED_HOSTS" ]; then
    echo "ERROR: Passwordless SSH setup FAILED for:$FAILED_HOSTS"
    echo "If sshd on these hosts has 'PasswordAuthentication no' (check /etc/ssh/sshd_config.d/),"
    echo "re-run with a sudo-capable user and key: $0 -u <user> -k <key.pem>"
    exit 1
fi

echo "Passwordless SSH setup completed for all nodes."
