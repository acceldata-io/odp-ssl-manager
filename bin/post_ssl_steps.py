#!/usr/bin/env python3
"""Steps that have to run AFTER SSL was enabled and the services were restarted.

ssl_manager.py only edits Ambari configs. These parts of an SSL setup are not config edits (ODP-8357, ODP-8358):

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
                to the TLS endpoints (only where the https endpoint answers) and give the NiFi repos their client
                keystore (update_ranger_repos.py).
  nifi-ranger   NiFi / NiFi Registry over https authorize every request through Ranger. Ranger Admin's lookup
                (Test Connection) needs READ for its client certificate identity, and the NiFi nodes (each with its
                own certificate) need /proxy WRITE or NiFi answers "Authentication Proxy Server not trusted".
                Creates the Ranger users and policies from the certificates found on the hosts.

Nothing is assumed about host names, IPs, ports, JDK location or the ssh user: hosts come from Ambari, tools are looked
up on each host, ports from the cluster configs or by probing. Every step is idempotent and only acts on installed
services; a failing step logs a warning and the others still run. Run it once the services are up on the new configs:

  bin/post_ssl_steps.py --host <ambari> --port 8446 --protocol https --user admin --password <pw> \\
      --cluster <name> --properties conf/ca.properties --scpKeyFile <ssh key> [--scpUserName root] \\
      [--steps all|trino-ca,knox,infra-solr,ranger-repos,nifi-ranger] [--dry-run] [--no-restart]
"""
import argparse
import base64
import configparser
import io
import json
import logging
import os
import secrets
import shlex
import ssl
import string
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import update_ranger_repos  # noqa: E402

ALL_STEPS = ["trino-ca", "knox", "infra-solr", "ranger-repos", "nifi-ranger"]
log = logging.getLogger("post_ssl_steps")

# bash helpers prepended to every remote script: find keytool and JAVA_HOME without assuming where the JDK is
PRELUDE = r'''
find_keytool() {
  local c
  for c in "$(command -v keytool 2>/dev/null)" "${JAVA_HOME:-/nonexistent}/bin/keytool" \
           "$(dirname "$(dirname "$(readlink -f "$(command -v java 2>/dev/null)" 2>/dev/null)")" 2>/dev/null)/bin/keytool" \
           /usr/lib/jvm/*/bin/keytool /opt/*/bin/keytool; do
    [ -n "$c" ] && [ -x "$c" ] && { echo "$c"; return; }
  done
}
KT=$(find_keytool)
JAVA_HOME_GUESS=$([ -n "$KT" ] && dirname "$(dirname "$(readlink -f "$KT")")")
'''


class Ambari:
    def __init__(self, host, port, protocol, user, password, cluster):
        self.base = "%s://%s:%s/api/v1/clusters/%s" % (protocol, host, port, cluster)
        self.ctx = ssl._create_unverified_context() if protocol == "https" else None
        self.hdr = {"Authorization": "Basic " + base64.b64encode(("%s:%s" % (user, password)).encode()).decode(),
                    "X-Requested-By": "ambari"}
        self.dry_run = False
        self.ranger_url = None
        self.ranger_cred = None
        self.assume_reachable = False
        self.cert_dir = "/etc/security/certificates"

    @property
    def keystore(self):
        return self.cert_dir + "/keystore.jks"

    @property
    def truststore(self):
        return self.cert_dir + "/truststore.jks"

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
        """Run a bash script on the host (as root through sudo unless the user is root); the script goes through stdin."""
        cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "LogLevel=ERROR", "-o", "ConnectTimeout=15", "-o", "BatchMode=yes"]
        if self.ssh_config:
            cmd += ["-F", os.path.expanduser(self.ssh_config)]
        if self.key:
            cmd += ["-i", os.path.expanduser(self.key)]
        cmd += ["%s@%s" % (self.user, host), "bash -s" if self.user == "root" else "sudo bash -s"]
        try:
            r = subprocess.run(cmd, input=(PRELUDE + script).encode(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        except subprocess.TimeoutExpired:
            return 124, "ssh timed out"
        return r.returncode, r.stdout.decode(errors="replace").strip()


class RangerApi:
    """Minimal Ranger Admin REST client (basic auth, certificate not validated: private CA)."""

    def __init__(self, url, user, password):
        self.url, self.user, self.password = url.rstrip("/"), user, password
        self.ctx = ssl._create_unverified_context()
        self.hdr = {"Authorization": "Basic " + base64.b64encode(("%s:%s" % (user, password)).encode()).decode(),
                    "Accept": "application/json", "Content-Type": "application/json"}

    def call(self, method, path, body=None):
        req = urllib.request.Request(self.url + path, method=method, headers=self.hdr,
                                     data=json.dumps(body).encode() if body is not None else None)
        text = urllib.request.urlopen(req, context=self.ctx, timeout=60).read().decode()
        return json.loads(text) if text.strip() else {}

    def services(self):
        return [s for s in self.call("GET", "/service/public/v2/api/service") if not s["name"].endswith("_tag")]

    def ensure_user(self, name):
        found = self.call("GET", "/service/xusers/users?name=" + urllib.parse.quote(name)).get("vXUsers") or []
        if any(u.get("name") == name for u in found):
            return False
        # the identity only authenticates with its certificate: the password is random, used once and not kept
        pw = "Aa1" + "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(24))
        self.call("POST", "/service/xusers/secure/users", {"name": name, "password": pw, "firstName": "odp-cert-identity", "lastName": "ssl",
                                                           "description": "certificate identity created by post_ssl_steps (NiFi / Ranger lookups)",
                                                           "userRoleList": ["ROLE_USER"], "status": 1})
        return True

    def ensure_policy(self, service, name, resource, value, access, users):
        """Give `users` the `access` on `value` of `service`. Ranger allows one policy per resource, so an existing policy
        (found by resource, or by name) is extended and only a missing one is created. Returns True when something changed."""
        users = sorted(set(users))
        found = self.call("GET", "/service/public/v2/api/service/%s/policy" % urllib.parse.quote(service))
        policies = found if isinstance(found, list) else []
        match = [p for p in policies if (p.get("resources", {}).get(resource, {}).get("values") or []) == [value]
                 and not p.get("resources", {}).get(resource, {}).get("isExcludes")]
        match = match or [p for p in policies if p.get("name") == name]
        if match:
            p = match[0]
            items = p.get("policyItems") or []
            item = next((i for i in items if any(x.get("type") == access and x.get("isAllowed", True) for x in i.get("accesses", []))), None)
            if item is None:
                item = {"users": [], "accesses": [{"type": access, "isAllowed": True}], "delegateAdmin": False}
                items.append(item)
            if set(users) <= set(item.get("users", [])):
                return False
            item["users"] = sorted(set(item.get("users", [])) | set(users))
            p["policyItems"] = items
            self.call("PUT", "/service/public/v2/api/policy/%s" % p["id"], p)
            return True
        self.call("POST", "/service/public/v2/api/policy", {
            "service": service, "name": name, "description": "created by post_ssl_steps: certificate identities of the NiFi / Ranger lookup path",
            "isEnabled": True, "isAuditEnabled": True,
            "resources": {resource: {"values": [value], "isExcludes": False, "isRecursive": False}},
            "policyItems": [{"users": users, "accesses": [{"type": access, "isAllowed": True}], "delegateAdmin": False}]})
        return True


def ca_password(properties, key):
    cfg = configparser.RawConfigParser()
    cfg.optionxform = str
    with open(properties) as f:
        cfg.read_file(io.StringIO(f.read()))
    return base64.b64decode(cfg.get("caprops", key)).decode().strip()


def ranger_connection(a, comps):
    """RangerApi for the first credential Ranger Admin accepts, or None."""
    admins = comps.get("RANGER_ADMIN")
    if not admins:
        return None
    _, adm, _ = a.config("admin-properties")
    _, renv, _ = a.config("ranger-env")
    url = (a.ranger_url or adm.get("policymgr_external_url") or "https://%s:6182" % admins[0]).rstrip("/")
    # Ambari creates its own admin-role user in Ranger (ranger_admin_username); admin_username is the install-time default
    candidates = [(renv.get("ranger_admin_username"), renv.get("ranger_admin_password")),
                  (renv.get("admin_username") or "admin", renv.get("admin_password"))]
    candidates = [(u, p) for u, p in candidates if u and p]
    if a.ranger_cred and a.ranger_cred[1]:
        candidates = [a.ranger_cred]
    for user, pw in candidates:
        api = RangerApi(url, user, pw)
        try:
            api.services()
            return api
        except urllib.error.HTTPError as e:
            if e.code not in (401, 403):
                raise
            log.info("ranger: %s not accepted by Ranger Admin (HTTP %s), trying the next credential", user, e.code)
    log.warning("ranger: no Ranger admin credential was accepted")
    return None


# --------------------------------------------------------------------------------------------- steps
def step_trino_ca(a, remote, comps, ctx):
    hosts = sorted(set(comps.get("TRINO_COORDINATOR", []) + comps.get("TRINO_WORKER", [])))
    if not hosts:
        return set()
    _, env, _ = a.config("trino-env")
    cacerts_pw = env.get("java_truststore_password") or "changeit"
    script = r'''
export TSPW=@TSPW@ CAPW=@CAPW@
CFG=$(ls /etc/trino/conf/config.properties /etc/trino*/conf/config.properties /usr/*/current/trino*/conf/config.properties 2>/dev/null | head -1)
TS=$([ -n "$CFG" ] && grep -m1 '^http-server.https.truststore.path=' "$CFG" | cut -d= -f2-)
[ -n "$TS" ] || { echo "SKIP no http-server.https.truststore.path"; exit 0; }
case "$TS" in */cacerts) ;; *) echo "SKIP truststore $TS is not a JDK cacerts"; exit 0;; esac
[ -f @TRUSTSTORE@ ] || { echo "SKIP no cluster truststore on this host"; exit 0; }
KT2=""
for d in "$(dirname "$(dirname "$(dirname "$TS")")")" "$(dirname "$(dirname "$(dirname "$(dirname "$TS")")")")"; do [ -x "$d/bin/keytool" ] && KT2="$d/bin/keytool" && break; done
[ -n "$KT2" ] && KT="$KT2"
[ -n "$KT" ] || { echo "SKIP no keytool"; exit 0; }
changed=0
for A in $($KT -list -keystore @TRUSTSTORE@ -storepass "$TSPW" 2>/dev/null | awk -F, '/trustedCertEntry/{print $1}'); do
  FP=$($KT -list -keystore @TRUSTSTORE@ -storepass "$TSPW" -alias "$A" 2>/dev/null | grep -m1 'SHA-256' | sed 's/^[^:]*: *//')
  if [ -n "$FP" ] && $KT -list -keystore "$TS" -storepass "$CAPW" 2>/dev/null | grep -qF "$FP"; then continue; fi
  $KT -exportcert -rfc -alias "$A" -keystore @TRUSTSTORE@ -storepass "$TSPW" -file "/tmp/odp_ca_$A.pem" 2>/dev/null
  if $KT -importcert -noprompt -alias "odp-$A" -file "/tmp/odp_ca_$A.pem" -keystore "$TS" -storepass "$CAPW" >/dev/null 2>&1; then echo "IMPORTED $A"; changed=1; else echo "FAILED $A (cacerts password?)"; fi
  rm -f "/tmp/odp_ca_$A.pem"
done
[ $changed = 0 ] && echo "UNCHANGED"
'''.replace("@TSPW@", shlex.quote(ctx["ts_pw"])).replace("@CAPW@", shlex.quote(cacerts_pw)).replace("@TRUSTSTORE@", shlex.quote(a.truststore))
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
    _, kenv, _ = a.config("knox-env")
    knox_user = kenv.get("knox_user") or "knox"
    aliases = ("gateway-identity-keystore-password", "gateway-identity-passphrase")
    script = r'''
export KSPW=@KSPW@
K=$(ls /usr/*/current/knox-server/bin/knoxcli.sh 2>/dev/null | head -1)
[ -n "$K" ] && [ -f @KEYSTORE@ ] && [ -n "$KT" ] || { echo "SKIP knox, keystore or keytool missing"; exit 0; }
echo "KEYALIAS=$($KT -list -keystore @KEYSTORE@ -storepass "$KSPW" 2>/dev/null | awk -F, '/PrivateKeyEntry/{print $1; exit}')"
EXISTING=$(sudo -u @KNOXUSER@ env JAVA_HOME=$JAVA_HOME_GUESS "$K" list-alias 2>/dev/null)
for AL in @ALIASES@; do
  if echo "$EXISTING" | grep -qix "$AL"; then echo "PRESENT $AL"; continue; fi
  OUT=$(sudo -u @KNOXUSER@ env JAVA_HOME=$JAVA_HOME_GUESS "$K" create-alias $AL --value "$KSPW" 2>&1 | tail -1)
  case "$OUT" in *"successfully created"*) echo "CREATED $AL";; *) echo "FAILED $AL: $OUT";; esac
done
'''.replace("@KSPW@", shlex.quote(ctx["ks_pw"])).replace("@KEYSTORE@", shlex.quote(a.keystore)).replace("@KNOXUSER@", shlex.quote(knox_user)).replace("@ALIASES@", " ".join(aliases))
    key_alias, restart, created = "", set(), False
    for h in hosts:
        if a.dry_run:
            log.info("[dry-run] knox: would create aliases and use %s on %s", a.keystore, h)
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
        "gateway.tls.keystore.path": a.keystore, "gateway.tls.keystore.type": "JKS", "gateway.tls.key.alias": key_alias,
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
    _, zoo, _ = a.config("zoo.cfg")
    port = str(zoo.get("clientPort") or "2181")
    zk = sorted(comps["ZOOKEEPER_SERVER"])[0]
    script = r'''
ZK=$(ls /usr/*/current/zookeeper-client/bin/zkCli.sh 2>/dev/null | head -1)
[ -n "$ZK" ] || { echo "DOWN=?"; exit 0; }
export JAVA_HOME=$JAVA_HOME_GUESS
LS=$(timeout 40 $ZK -server @ZK@:@PORT@ ls @ZNODE@/collections 2>/dev/null | grep '^\[')
[ -n "$LS" ] || { echo "DOWN=?"; exit 0; }
down=0
for C in $(echo "$LS" | tr -d '[],'); do
  N=$(timeout 40 $ZK -server @ZK@:@PORT@ get @ZNODE@/collections/$C/state.json 2>/dev/null | tr -d '\n ' | grep -o '"state":"down"' | wc -l)
  [ "$N" -gt 0 ] && echo "DOWN $C"; down=$((down+N))
done
echo "DOWN=$down"
'''.replace("@ZK@", shlex.quote(zk)).replace("@PORT@", shlex.quote(port)).replace("@ZNODE@", shlex.quote(znode))
    if a.dry_run:
        log.info("[dry-run] infra-solr: would check replica states on %s", zk)
        return set()
    rc, out = remote.run(zk, script, timeout=400)
    log.info("infra-solr %s: %s", zk, out.replace("\n", "; "))
    if "DOWN=0" in out or "DOWN=?" in out or "DOWN=" not in out:
        return set()
    return {"AMBARI_INFRA_SOLR"}


def step_ranger_repos(a, remote, comps, ctx):
    api = ranger_connection(a, comps)
    if api is None:
        return set()
    kafka = ""
    for t in ("kafka3-broker", "kraft4-broker", "kafka-broker"):
        proto = (a.config(t)[1].get("security.inter.broker.protocol") or "").strip()
        if proto in ("SSL", "SASL_SSL"):
            kafka = proto
            break
    rpc = (a.config("core-site")[1].get("hadoop.rpc.protection") or "").strip()
    nifi_ssl = {"keystore": a.keystore, "keystore_password": ctx["ks_pw"], "truststore": a.truststore, "truststore_password": ctx["ts_pw"]}
    n = update_ranger_repos.sync_repos(api.url, api.user, api.password, kafka, a.truststore, rpc, apply=not a.dry_run, nifi_ssl=nifi_ssl,
                                       reachable=(lambda h, p: True) if a.assume_reachable else update_ranger_repos.tls_reachable,
                                       out=lambda m: log.info("ranger-repos: %s", m.replace(ctx["ks_pw"], "<hidden>")))
    log.info("ranger-repos: %d repositories %s", n, "would change (dry run)" if a.dry_run else "moved to the TLS endpoints")
    return set()


IDENT_SCRIPT = r'''
export KSPW=@KSPW@
P=$(ls @PROPS@ 2>/dev/null | head -1)
KS=@FALLBACK_KS@; PW="$KSPW"
if [ -n "$P" ]; then
  K1=$(grep -m1 '^@PFX@security.keystore=' "$P" | cut -d= -f2-); [ -n "$K1" ] && [ -f "$K1" ] && KS="$K1"
  P1=$(grep -m1 '^@PFX@security.keystorePasswd=' "$P" | cut -d= -f2-)
  case "$P1" in ""|enc\{*|*"{{"*|*"{%"*) ;; *) PW="$P1";; esac
fi
[ -n "$KT" ] && [ -f "$KS" ] || { echo "SKIP no keytool or keystore"; exit 0; }
OWNER=$($KT -list -v -keystore "$KS" -storepass "$PW" 2>/dev/null | awk '/Entry type: PrivateKeyEntry/{f=1} f && /^Owner:/{sub(/^Owner: */,""); print; exit}')
[ -z "$OWNER" ] && OWNER=$($KT -list -v -keystore "$KS" -storepass "$KSPW" 2>/dev/null | awk '/^Owner:/{sub(/^Owner: */,""); print; exit}')
echo "IDENT=$OWNER"
'''


def _identity(a, remote, host, props_glob, prefix, ctx):
    script = IDENT_SCRIPT.replace("@KSPW@", shlex.quote(ctx["ks_pw"])).replace("@PROPS@", props_glob).replace("@PFX@", prefix).replace(
        "@FALLBACK_KS@", shlex.quote(a.keystore))
    rc, out = remote.run(host, script)
    for line in out.splitlines():
        if line.startswith("IDENT=") and line[6:].strip():
            return line[6:].strip()
    log.warning("nifi-ranger: no certificate identity found on %s (%s)", host, out.replace("\n", "; ")[:150] or "rc=%s" % rc)
    return None


def step_nifi_ranger(a, remote, comps, ctx):
    api = ranger_connection(a, comps)
    if api is None:
        return set()
    repos = {s["type"]: s for s in api.services() if s.get("type") in update_ranger_repos.NIFI_TYPES}
    if not repos:
        return set()
    nifi_hosts = sorted({h for c, hs in comps.items() if c.startswith("NIFI") and "REGISTRY" not in c and c.endswith(("_MASTER", "_NODE")) for h in hs})
    reg_hosts = sorted({h for c, hs in comps.items() if "NIFI_REGISTRY" in c and c.endswith(("_MASTER", "_NODE")) for h in hs})
    if a.dry_run:
        log.info("[dry-run] nifi-ranger: would read the certificate identities of %s and of the Ranger Admin client and create the Ranger users/policies",
                 ", ".join(nifi_hosts + reg_hosts) or "(no NiFi hosts)")
        return set()
    # the Ranger Admin client identity = subject of the keystore Ranger Admin presents to NiFi (repo nifi.ssl.keystore)
    client = _identity(a, remote, comps["RANGER_ADMIN"][0], "/nonexistent-properties-file", "none.", ctx)
    if not client:
        return set()
    plan = []   # (repo type, hosts, properties file glob, property prefix)
    if "nifi" in repos and (repos["nifi"]["configs"].get("nifi.url") or "").startswith("https://"):
        plan.append(("nifi", nifi_hosts, "/etc/nifi*/conf/nifi.properties", "nifi."))
    if "nifi-registry" in repos and (repos["nifi-registry"]["configs"].get("nifi.registry.url") or "").startswith("https://"):
        plan.append(("nifi-registry", reg_hosts, "/etc/nifi-registry*/conf/nifi-registry.properties", "nifi.registry."))
    created = 0
    for rtype, hosts, props, prefix in plan:
        svc = repos[rtype]["name"]
        nodes = [i for i in (_identity(a, remote, h, props, prefix, ctx) for h in hosts) if i]
        resdef = api.call("GET", "/service/public/v2/api/servicedef/name/" + rtype)
        res = (resdef.get("resources") or [{}])[0].get("name")
        if not res:
            log.warning("nifi-ranger: no resource definition for %s", rtype)
            continue
        everyone = sorted(set([client] + nodes))
        for ident in everyone:
            if api.ensure_user(ident):
                created += 1
                log.info("nifi-ranger: Ranger user created for %s", ident)
        lookup = "/resources" if rtype == "nifi" else "/policies"
        for name, value, access, users in (("ODP post-SSL: Ranger Admin lookup", lookup, "READ", [client]),
                                           ("ODP post-SSL: node proxy", "/proxy", "WRITE", everyone)):
            try:
                if api.ensure_policy(svc, name, res, value, access, users):
                    created += 1
                    log.info("nifi-ranger: %s policy '%s' (%s %s) for %s", svc, name, access, value, ", ".join(users))
            except urllib.error.HTTPError as e:
                log.warning("nifi-ranger: policy '%s' on %s rejected (HTTP %s): %s", name, svc, e.code, e.read().decode()[:160])
    log.info("nifi-ranger: %d object(s) created or extended", created)
    return set()


STEPS = {"trino-ca": step_trino_ca, "knox": step_knox, "infra-solr": step_infra_solr, "ranger-repos": step_ranger_repos,
         "nifi-ranger": step_nifi_ranger}


def run(o, remote=None, ambari=None):
    """Run the selected steps. Returns the set of services that were (or would be) restarted."""
    steps = ALL_STEPS if o.steps == "all" else [s.strip() for s in o.steps.split(",") if s.strip()]
    a = ambari or Ambari(o.host, o.port, o.protocol, o.user, o.password, o.cluster)
    a.dry_run = o.dry_run
    a.ranger_url = o.ranger_url
    a.ranger_cred = (o.ranger_user, os.environ.get(o.ranger_password_env)) if o.ranger_user else None
    a.cert_dir = o.cert_dir.rstrip("/")
    a.assume_reachable = o.assume_reachable
    remote = remote or Remote(o.scpKeyFile, o.scpUserName, o.ssh_config)
    ctx = {"ks_pw": ca_password(o.properties, "keyStorePassword"), "ts_pw": ca_password(o.properties, "trustStorePassword")}
    for item in o.port_map:
        plain, _, tls = item.partition("=")
        update_ranger_repos.add_port_candidate(plain, tls)
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
    return restart


def parser():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", required=True, help="Ambari server host")
    ap.add_argument("--port", default="8080")
    ap.add_argument("--protocol", default="http", choices=["http", "https"])
    ap.add_argument("--user", default="admin")
    ap.add_argument("--password", default="admin")
    ap.add_argument("--cluster", required=True)
    ap.add_argument("--properties", required=True, help="ca.properties used for the SSL setup (keystore/truststore passwords)")
    ap.add_argument("--cert-dir", default="/etc/security/certificates", help="directory of keystore.jks / truststore.jks on the hosts")
    ap.add_argument("--scpKeyFile", help="ssh key for the cluster hosts")
    ap.add_argument("--scpUserName", default=os.environ.get("USER", "root"))
    ap.add_argument("--ssh-config", help="ssh config file (-F), e.g. to reach the hosts from outside the cluster network")
    ap.add_argument("--ranger-url", help="Ranger Admin URL when admin-properties policymgr_external_url is not reachable from here")
    ap.add_argument("--ranger-user", help="Ranger Admin user (default: the credentials Ambari stores in ranger-env)")
    ap.add_argument("--ranger-password-env", default="RANGER_PASSWORD", help="env var with the Ranger Admin password for --ranger-user")
    ap.add_argument("--port-map", action="append", default=[], metavar="PLAIN=TLS", help="extra plain->TLS port candidate for the repo endpoint check")
    ap.add_argument("--assume-reachable", action="store_true", help="do not probe the https endpoints of the Ranger repos")
    ap.add_argument("--steps", default="all", help="all or a comma separated list of: " + ",".join(ALL_STEPS))
    ap.add_argument("--dry-run", action="store_true", help="only report what would be done")
    ap.add_argument("--no-restart", action="store_true", help="do not restart the services a step changed")
    return ap


def main(argv=None):
    ap = parser()
    o = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    steps = ALL_STEPS if o.steps == "all" else [s.strip() for s in o.steps.split(",") if s.strip()]
    bad = [s for s in steps if s not in STEPS]
    if bad:
        ap.error("unknown step(s): %s" % ", ".join(bad))
    run(o)
    return 0


if __name__ == "__main__":
    sys.exit(main())
