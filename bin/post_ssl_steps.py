#!/usr/bin/env python3.11
"""Steps that have to run AFTER SSL was enabled and the services were restarted.

ssl_manager.py only edits Ambari configs. These parts of an SSL setup are not config edits (ODP-8357):

  trino-ca      Trino nodes validate each other with the JVM default trust (JDK cacerts). With the cluster
                certificate (private CA) the workers cannot announce to the coordinator and every query stays
                queued. Import the cluster truststore certificates into that cacerts, restart Trino.
  knox          Knox serves a self-signed gateway certificate that Ranger Admin and clients do not trust. Point
                gateway-site at the cluster keystore (gateway.tls.*), store its password in the Knox credential
                store (knoxcli), restart Knox.
  infra-solr    After the first SSL start the single replica of ranger_audits / atlas indexes can stay "down" in
                ZooKeeper ("conflicting information about the leader ... http vs https"), which breaks the Ranger
                Audit tab and Atlas indexing. Restart Infra Solr when a replica is down.
  ranger-repos  Ranger repositories keep the http endpoints they were created with (Test Connection fails), Kafka
                repos stay SASL_PLAINTEXT and the HDFS repo keeps hadoop.rpc.protection=authentication. Move them
                to the TLS endpoints (update_ranger_repos.py).

Every step is idempotent and only acts on installed services; a failing step logs a warning and the others still
run. Run it once the services are up on the new configs:

  bin/post_ssl_steps.py --host <ambari> --port 8446 --protocol https --user admin --password <pw> \\
      --cluster <name> --properties conf/ca.properties --scpKeyFile <ssh key> [--scpUserName root] \\
      [--steps all|trino-ca,knox,infra-solr,ranger-repos] [--dry-run] [--no-restart]
"""
import argparse
import base64
import configparser
import io
import json
import logging
import os
import shlex
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import update_ranger_repos  # noqa: E402

CERT_DIR = "/etc/security/certificates"
KEYSTORE = CERT_DIR + "/keystore.jks"
TRUSTSTORE = CERT_DIR + "/truststore.jks"
ALL_STEPS = ["trino-ca", "knox", "infra-solr", "ranger-repos"]
log = logging.getLogger("post_ssl_steps")


class Ambari:
    def __init__(self, host, port, protocol, user, password, cluster):
        self.base = "%s://%s:%s/api/v1/clusters/%s" % (protocol, host, port, cluster)
        self.ctx = ssl._create_unverified_context() if protocol == "https" else None
        self.hdr = {"Authorization": "Basic " + base64.b64encode(("%s:%s" % (user, password)).encode()).decode(),
                    "X-Requested-By": "ambari"}

    def call(self, method, path, body=None):
        req = urllib.request.Request(self.base + path, method=method, headers=self.hdr,
                                     data=json.dumps(body).encode() if body is not None else None)
        text = urllib.request.urlopen(req, context=self.ctx, timeout=120).read().decode()
        return json.loads(text) if text.strip() else {}

    def components(self):
        out = {}
        for i in self.call("GET", "/host_components?fields=HostRoles/component_name,HostRoles/host_name")["items"]:
            out.setdefault(i["HostRoles"]["component_name"], []).append(i["HostRoles"]["host_name"])
        return out

    def config(self, config_type):
        try:
            tag = self.call("GET", "?fields=Clusters/desired_configs/" + config_type)["Clusters"]["desired_configs"][config_type]["tag"]
            items = self.call("GET", "/configurations?type=%s&tag=%s" % (config_type, tag))["items"]
            return tag, (items[0].get("properties", {}) if items else {}), (items[0].get("properties_attributes", {}) if items else {})
        except Exception:
            return None, {}, {}

    def set_config(self, config_type, updates, note):
        tag, props, attrs = self.config(config_type)
        if tag is None:
            log.warning("config type %s not found, skipped", config_type)
            return False
        changed = {k: v for k, v in updates.items() if props.get(k) != v}
        if not changed:
            return False
        props = dict(props)
        props.update(changed)
        self.call("PUT", "", [{"Clusters": {"desired_config": [{"type": config_type, "tag": "version" + str(int(time.time() * 1000)),
                                                                 "properties": props, "properties_attributes": attrs,
                                                                 "service_config_version_note": note}]}}])
        log.info("%s: updated %s", config_type, ", ".join(sorted(changed)))
        return True

    def restart(self, service):
        for state, verb in (("INSTALLED", "Stop"), ("STARTED", "Start")):
            r = self.call("PUT", "/services/%s" % service, {"RequestInfo": {"context": "%s %s (post SSL steps)" % (verb, service)},
                                                           "Body": {"ServiceInfo": {"state": state}}})
            if not r:
                continue
            rid = r["Requests"]["id"]
            while True:
                status = self.call("GET", "/requests/%s" % rid)["Requests"]["request_status"]
                if status not in ("PENDING", "IN_PROGRESS", "QUEUED"):
                    break
                time.sleep(8)
            log.info("%s %s: %s", verb, service, status)


class Remote:
    def __init__(self, key, user, ssh_config=None):
        self.key, self.user, self.ssh_config = key, user, ssh_config

    def run(self, host, script, timeout=300):
        """Run a bash script on the host (as root through sudo unless the user is root); script goes through stdin."""
        cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "LogLevel=ERROR", "-o", "ConnectTimeout=15", "-o", "BatchMode=yes"]
        if self.ssh_config:
            cmd += ["-F", os.path.expanduser(self.ssh_config)]
        if self.key:
            cmd += ["-i", os.path.expanduser(self.key)]
        cmd += ["%s@%s" % (self.user, host), "bash -s" if self.user == "root" else "sudo bash -s"]
        r = subprocess.run(cmd, input=script.encode(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        return r.returncode, r.stdout.decode(errors="replace").strip()


def ca_password(properties, key):
    cfg = configparser.RawConfigParser()
    cfg.optionxform = str
    with open(properties) as f:
        cfg.read_file(io.StringIO(f.read()))
    return base64.b64decode(cfg.get("caprops", key)).decode().strip()


# --------------------------------------------------------------------------------------------- steps
def step_trino_ca(a, remote, comps, ctx):
    hosts = sorted(set(comps.get("TRINO_COORDINATOR", []) + comps.get("TRINO_WORKER", [])))
    if not hosts:
        return set()
    _, env, _ = a.config("trino-env")
    cacerts_pw = (env.get("java_truststore_password") or "changeit")
    script = r'''
export TSPW=%s CAPW=%s
CFG=/etc/trino/conf/config.properties
TS=$(grep -m1 '^http-server.https.truststore.path=' $CFG 2>/dev/null | cut -d= -f2-)
[ -n "$TS" ] || { echo "SKIP no http-server.https.truststore.path"; exit 0; }
case "$TS" in */cacerts) ;; *) echo "SKIP truststore $TS is not a JDK cacerts"; exit 0;; esac
[ -f %s ] || { echo "SKIP no cluster truststore on this host"; exit 0; }
KT=$(dirname $(dirname $(dirname $TS)))/bin/keytool; [ -x $KT ] || KT=$(command -v keytool)
changed=0
for A in $($KT -list -keystore %s -storepass "$TSPW" 2>/dev/null | awk -F, '/trustedCertEntry/{print $1}'); do
  FP=$($KT -list -keystore %s -storepass "$TSPW" -alias "$A" 2>/dev/null | grep -m1 'SHA-256' | sed 's/^[^:]*: *//')
  if [ -n "$FP" ] && $KT -list -keystore "$TS" -storepass "$CAPW" 2>/dev/null | grep -qF "$FP"; then continue; fi
  $KT -exportcert -rfc -alias "$A" -keystore %s -storepass "$TSPW" -file /tmp/odp_ca_$A.pem 2>/dev/null
  if $KT -importcert -noprompt -alias "odp-$A" -file /tmp/odp_ca_$A.pem -keystore "$TS" -storepass "$CAPW" >/dev/null 2>&1; then echo "IMPORTED $A"; changed=1; else echo "FAILED $A (cacerts password?)"; fi
  rm -f /tmp/odp_ca_$A.pem
done
[ $changed = 0 ] && echo "UNCHANGED"
''' % (shlex.quote(ctx["ts_pw"]), shlex.quote(cacerts_pw), TRUSTSTORE, TRUSTSTORE, TRUSTSTORE, TRUSTSTORE)
    restart = set()
    for h in hosts:
        if a.dry_run:
            log.info("[dry-run] trino-ca: would check %s", h)
            continue
        rc, out = remote.run(h, script)
        log.info("trino-ca %s: %s", h, out.replace("\n", "; ") or "no output (rc=%s)" % rc)
        if "IMPORTED" in out:
            restart.add("TRINO")
    return restart


def step_knox(a, remote, comps, ctx):
    hosts = sorted(set(comps.get("KNOX_GATEWAY", [])))
    if not hosts:
        return set()
    aliases = ("gateway-identity-keystore-password", "gateway-identity-passphrase")
    script = r'''
export KSPW=%s
K=/usr/odp/current/knox-server/bin/knoxcli.sh
[ -x $K ] && [ -f %s ] || { echo "SKIP knox or keystore missing"; exit 0; }
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(command -v java))))
KT=$JAVA_HOME/bin/keytool
echo "KEYALIAS=$($KT -list -keystore %s -storepass "$KSPW" 2>/dev/null | awk -F, '/PrivateKeyEntry/{print $1; exit}')"
EXISTING=$(sudo -u knox env JAVA_HOME=$JAVA_HOME $K list-alias 2>/dev/null)
for AL in %s; do
  if echo "$EXISTING" | grep -qix "$AL"; then echo "PRESENT $AL"; continue; fi
  OUT=$(sudo -u knox env JAVA_HOME=$JAVA_HOME $K create-alias $AL --value "$KSPW" 2>&1 | tail -1)
  case "$OUT" in *"successfully created"*) echo "CREATED $AL";; *) echo "FAILED $AL: $OUT";; esac
done
''' % (shlex.quote(ctx["ks_pw"]), KEYSTORE, KEYSTORE, " ".join(aliases))
    key_alias, restart, created = "", set(), False
    for h in hosts:
        if a.dry_run:
            log.info("[dry-run] knox: would create aliases and use %s on %s", KEYSTORE, h)
            continue
        rc, out = remote.run(h, script)
        log.info("knox %s: %s", h, out.replace("\n", "; ") or "no output (rc=%s)" % rc)
        for line in out.splitlines():
            if line.startswith("KEYALIAS=") and line[9:]:
                key_alias = line[9:]
        created = created or "CREATED" in out
    if a.dry_run or not key_alias:
        return restart
    changed = a.set_config("gateway-site", {
        "gateway.tls.keystore.path": KEYSTORE, "gateway.tls.keystore.type": "JKS", "gateway.tls.key.alias": key_alias,
        "gateway.tls.keystore.password.alias": aliases[0], "gateway.tls.key.passphrase.alias": aliases[1]},
        "ODP-8357: Knox uses the cluster keystore")
    if changed or created:
        restart.add("KNOX")
    return restart


def step_infra_solr(a, remote, comps, ctx):
    if not comps.get("INFRA_SOLR") or not comps.get("ZOOKEEPER_SERVER"):
        return set()
    _, env, _ = a.config("infra-solr-env")
    znode = env.get("infra_solr_znode") or "/infra-solr"
    zk = sorted(comps["ZOOKEEPER_SERVER"])[0]
    script = r'''
ZK=/usr/odp/current/zookeeper-client/bin/zkCli.sh
[ -x $ZK ] || { echo "DOWN=?"; exit 0; }
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(command -v java))))
down=0
for C in $(timeout 40 $ZK -server %s:2181 ls %s/collections 2>/dev/null | grep '^\[' | tr -d '[],'); do
  N=$(timeout 40 $ZK -server %s:2181 get %s/collections/$C/state.json 2>/dev/null | tr -d '\n ' | grep -o '"state":"down"' | wc -l)
  [ "$N" -gt 0 ] && echo "DOWN $C"; down=$((down+N))
done
echo "DOWN=$down"
''' % (zk, znode, zk, znode)
    if a.dry_run:
        log.info("[dry-run] infra-solr: would check replica states on %s", zk)
        return set()
    rc, out = remote.run(zk, script, timeout=400)
    log.info("infra-solr %s: %s", zk, out.replace("\n", "; "))
    if "DOWN=0" in out or "DOWN=?" in out or "DOWN=" not in out:
        return set()
    return {"AMBARI_INFRA_SOLR"}


def step_ranger_repos(a, remote, comps, ctx):
    admins = comps.get("RANGER_ADMIN")
    if not admins:
        return set()
    _, adm, _ = a.config("admin-properties")
    _, renv, _ = a.config("ranger-env")
    url = (a.ranger_url or adm.get("policymgr_external_url") or "https://%s:6182" % admins[0]).rstrip("/")
    # Ambari creates its own admin-role user in Ranger (ranger_admin_username); admin_username is the install-time default
    candidates = [(renv.get("ranger_admin_username"), renv.get("ranger_admin_password")),
                  (renv.get("admin_username") or "admin", renv.get("admin_password"))]
    candidates = [(u, p) for u, p in candidates if u and p]
    if a.ranger_cred and a.ranger_cred[1]:
        candidates = [a.ranger_cred]
    if not candidates:
        log.warning("ranger-repos: no Ranger admin credentials in ranger-env, skipped")
        return set()
    kafka = ""
    for t in ("kafka3-broker", "kraft4-broker", "kafka-broker"):
        proto = (a.config(t)[1].get("security.inter.broker.protocol") or "").strip()
        if proto in ("SSL", "SASL_SSL"):
            kafka = proto
            break
    rpc = (a.config("core-site")[1].get("hadoop.rpc.protection") or "").strip()
    n = None
    for user, pw in candidates:
        try:
            n = update_ranger_repos.sync_repos(url, user, pw, kafka, TRUSTSTORE, rpc, apply=not a.dry_run,
                                               out=lambda m: log.info("ranger-repos: %s", m))
            break
        except urllib.error.HTTPError as e:
            if e.code not in (401, 403):
                raise
            log.info("ranger-repos: %s not accepted by Ranger Admin (HTTP %s), trying the next credential", user, e.code)
    if n is None:
        log.warning("ranger-repos: no Ranger admin credential from ranger-env was accepted, skipped")
        return set()
    log.info("ranger-repos: %d repositories %s", n, "would change (dry run)" if a.dry_run else "moved to the TLS endpoints")
    return set()


STEPS = {"trino-ca": step_trino_ca, "knox": step_knox, "infra-solr": step_infra_solr, "ranger-repos": step_ranger_repos}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", required=True, help="Ambari server host")
    ap.add_argument("--port", default="8080")
    ap.add_argument("--protocol", default="http", choices=["http", "https"])
    ap.add_argument("--user", default="admin")
    ap.add_argument("--password", default="admin")
    ap.add_argument("--cluster", required=True)
    ap.add_argument("--properties", required=True, help="ca.properties used for the SSL setup (keystore/truststore passwords)")
    ap.add_argument("--scpKeyFile", help="ssh key for the cluster hosts")
    ap.add_argument("--scpUserName", default=os.environ.get("USER", "root"))
    ap.add_argument("--ssh-config", help="ssh config file (-F), e.g. to reach the hosts from outside the cluster network")
    ap.add_argument("--ranger-url", help="Ranger Admin URL when admin-properties policymgr_external_url is not reachable from here")
    ap.add_argument("--ranger-user", help="Ranger Admin user (default: the credentials Ambari stores in ranger-env)")
    ap.add_argument("--ranger-password-env", default="RANGER_PASSWORD", help="env var with the Ranger Admin password for --ranger-user")
    ap.add_argument("--steps", default="all", help="all or a comma separated list of: " + ",".join(ALL_STEPS))
    ap.add_argument("--dry-run", action="store_true", help="only report what would be done")
    ap.add_argument("--no-restart", action="store_true", help="do not restart the services a step changed")
    o = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)

    steps = ALL_STEPS if o.steps == "all" else [s.strip() for s in o.steps.split(",") if s.strip()]
    bad = [s for s in steps if s not in STEPS]
    if bad:
        ap.error("unknown step(s): %s" % ", ".join(bad))
    a = Ambari(o.host, o.port, o.protocol, o.user, o.password, o.cluster)
    a.dry_run = o.dry_run
    a.ranger_url = o.ranger_url
    a.ranger_cred = (o.ranger_user, os.environ.get(o.ranger_password_env)) if o.ranger_user else None
    remote = Remote(o.scpKeyFile, o.scpUserName, o.ssh_config)
    ctx = {"ks_pw": ca_password(o.properties, "keyStorePassword"), "ts_pw": ca_password(o.properties, "trustStorePassword")}
    comps = a.components()
    restart = set()
    for s in steps:
        try:
            log.info("== step %s", s)
            restart |= STEPS[s](a, remote, comps, ctx)
        except Exception as e:  # a failing step must not stop the others
            log.warning("step %s failed: %s", s, e)
    if restart and not o.dry_run and not o.no_restart:
        for svc in sorted(restart):
            try:
                a.restart(svc)
            except Exception as e:
                log.warning("restart %s failed: %s", svc, e)
    elif restart:
        log.info("services to restart: %s", ", ".join(sorted(restart)))
    log.info("done")


if __name__ == "__main__":
    main()
