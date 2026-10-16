#!/bin/bash
# Runs post_ssl_steps.py once the "restart all stale components" request that the SSL job fires has finished.
# Usage (from the odp-ssl-manager checkout, as the SSL job's wrapper does):
#   bin/post_ssl_run.sh <ambari-host> <cluster> <ssh-key> [ambari-user] [ambari-password]
# Ambari is tried on https/8446 first, then http/8080. Never fails the calling job: a problem is logged and exit is 0.
AMB=$1; CLUSTER=$2; KEY=$3; AUSER=${4:-admin}; APASS=${5:-admin}
HERE=$(cd "$(dirname "$0")" && pwd)
[ -n "$AMB" ] && [ -n "$CLUSTER" ] || { echo "usage: $0 <ambari-host> <cluster> <ssh-key> [user] [password]"; exit 0; }

api() { curl -ksu "$AUSER:$APASS" -H 'X-Requested-By: ambari' "$PROTO://$AMB:$PORT/api/v1/clusters/$CLUSTER$1" 2>/dev/null; }
for cand in "https 8446" "http 8080"; do
  set -- $cand; PROTO=$1; PORT=$2
  api "" | grep -q cluster_name && break
  PROTO=; PORT=
done
[ -n "$PROTO" ] || { echo "post_ssl_run: Ambari not reachable on 8446/8080, skipping post-SSL steps"; exit 0; }

# wait (max 60 min) until no operation is running, i.e. the restart of the stale components is done
echo "post_ssl_run: waiting for running Ambari requests to finish"
for i in $(seq 1 360); do
  busy=$(api "/requests?to=end&page_size=10&fields=Requests/request_status&sortBy=Requests/id.desc" \
         | grep -c '"request_status" : "\(IN_PROGRESS\|PENDING\|QUEUED\)"')
  [ "$busy" = 0 ] && break
  sleep 10
done
# a few seconds so the component states settle
sleep 20

SSHOPT=()
[ -n "$KEY" ] && SSHOPT=(--scpKeyFile "$KEY")
"$HERE/post_ssl_steps.py" --host "$AMB" --port "$PORT" --protocol "$PROTO" --user "$AUSER" --password "$APASS" --cluster "$CLUSTER" \
  --properties "$HERE/../conf/ca.properties" "${SSHOPT[@]}" --scpUserName "${SSH_USER:-root}" \
  || echo "post_ssl_run: post_ssl_steps.py returned $? (continuing)"
exit 0
