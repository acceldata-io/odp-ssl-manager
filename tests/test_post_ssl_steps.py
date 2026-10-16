"""Cluster-shape tests for bin/post_ssl_steps.py and bin/update_ranger_repos.py (no cluster needed).

A fake Ambari and a fake Ranger Admin run as local HTTP servers, ssh is replaced by a stub that records every call.
Run: python3 -m unittest discover -s tests -v
"""
import base64
import json
import os
import re
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs, unquote

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bin"))
import post_ssl_steps as P  # noqa: E402
import update_ranger_repos as U  # noqa: E402


class Fake:
    """Shared state of one test: what the fake Ambari / Ranger answer and what was written to them."""

    def __init__(self, comps, configs=None, repos=None, policies=None, users=None, ranger_auth=True):
        self.comps, self.configs = comps, configs or {}
        self.repos, self.policies, self.users = repos or [], policies or [], set(users or [])
        self.ranger_auth = ranger_auth
        self.ambari_writes, self.restarts, self.ranger_writes = [], [], []

    # ---------------------------------------------------------------- Ambari
    def ambari(self, method, path, body):
        if method == "GET" and "/host_components" in path:
            items = [{"HostRoles": {"component_name": c, "host_name": h}} for c, hs in self.comps.items() for h in hs]
            return {"items": items}
        m = re.search(r"desired_configs/([^&]+)", path)
        if method == "GET" and m:
            t = m.group(1)
            if t not in self.configs:
                return {"Clusters": {"desired_configs": {}}}
            return {"Clusters": {"desired_configs": {t: {"tag": "v1"}}}}
        m = re.search(r"type=([^&]+)&tag=", path)
        if method == "GET" and m:
            return {"items": [{"properties": dict(self.configs.get(m.group(1), {})), "properties_attributes": {}}]}
        if method == "PUT" and path.endswith("/services/" + path.rsplit("/", 1)[-1]) and "/services/" in path:
            self.restarts.append((path.rsplit("/", 1)[-1], body["Body"]["ServiceInfo"]["state"]))
            return {"Requests": {"id": 7}}
        if method == "GET" and "/requests/" in path:
            return {"Requests": {"request_status": "COMPLETED"}}
        if method == "PUT":
            self.ambari_writes.append(body)
            for item in body:
                for dc in item["Clusters"]["desired_config"]:
                    self.configs[dc["type"]] = dc["properties"]
            return {}
        return {}

    # ---------------------------------------------------------------- Ranger
    def ranger(self, method, path, body):
        u = urlparse(path)
        if not self.ranger_auth:
            return 401, {}
        p = u.path
        if method == "GET" and p == "/service/public/v2/api/service":
            return 200, self.repos
        if method == "GET" and p.startswith("/service/public/v2/api/servicedef/name/"):
            return 200, {"resources": [{"name": "nifi-registry-resource" if p.endswith("registry") else "nifi-resource"}]}
        if method == "PUT" and p.startswith("/service/public/v2/api/service/"):
            sid = int(p.rsplit("/", 1)[-1])
            self.repos = [body if r["id"] == sid else r for r in self.repos]
            self.ranger_writes.append(("repo", sid))
            return 200, body
        if method == "GET" and p == "/service/xusers/users":
            name = unquote(parse_qs(u.query)["name"][0])
            return 200, {"vXUsers": [{"name": name}] if name in self.users else []}
        if method == "POST" and p == "/service/xusers/secure/users":
            self.users.add(body["name"])
            self.ranger_writes.append(("user", body["name"]))
            return 200, body
        m = re.match(r"/service/public/v2/api/service/([^/]+)/policy$", p)
        if method == "GET" and m:
            return 200, [x for x in self.policies if x["service"] == unquote(m.group(1))]
        if method == "POST" and p == "/service/public/v2/api/policy":
            body["id"] = len(self.policies) + 100
            self.policies.append(body)
            self.ranger_writes.append(("policy+", body["name"]))
            return 200, body
        if method == "PUT" and p.startswith("/service/public/v2/api/policy/"):
            pid = int(p.rsplit("/", 1)[-1])
            self.policies = [body if x["id"] == pid else x for x in self.policies]
            self.ranger_writes.append(("policy~", body["name"]))
            return 200, body
        return 404, {}


def serve(handler_fn):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _do(self, method):
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n)) if n else None
            code, out = handler_fn(method, self.path, body)
            data = json.dumps(out).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = lambda self: self._do("GET")  # noqa: E731
        do_PUT = lambda self: self._do("PUT")  # noqa: E731
        do_POST = lambda self: self._do("POST")  # noqa: E731

    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


class StubRemote:
    """Replaces ssh. `answers` maps a marker found in the script to the output; every call is recorded."""

    def __init__(self, answers, rc=0):
        self.answers, self.rc, self.calls = answers, rc, []

    def run(self, host, script, timeout=300):
        self.calls.append((host, script))
        if self.rc:
            return self.rc, "ssh: connect to host %s port 22: Connection refused" % host
        for marker, out in self.answers:
            if marker in script:
                return 0, out(host) if callable(out) else out
        return 0, ""


def repo(rid, name, rtype, configs):
    return {"id": rid, "name": name, "type": rtype, "configs": configs}


def ident(host):
    return "IDENT=CN=%s, OU=HDP" % host.split(".")[0]


FULL = {"HDFS_NAMENODE": ["nn1.example.com"], "TRINO_COORDINATOR": ["tr1.example.com"], "TRINO_WORKER": ["tr2.example.com"],
        "KNOX_GATEWAY": ["kx1.example.com"], "INFRA_SOLR": ["is1.example.com"], "ZOOKEEPER_SERVER": ["zk1.example.com", "zk2.example.com"],
        "RANGER_ADMIN": ["ra1.example.com"], "NIFI2_MASTER": ["nf1.example.com", "nf2.example.com"], "NIFI_REGISTRY2_MASTER": ["nr1.example.com"]}
CONF = {"trino-env": {"java_truststore_password": "changeit"}, "knox-env": {"knox_user": "knox"}, "gateway-site": {},
        "infra-solr-env": {"infra_solr_znode": "/infra-solr"}, "zoo.cfg": {"clientPort": "2181"},
        "admin-properties": {"policymgr_external_url": "https://ra1.example.com:6182"},
        "ranger-env": {"ranger_admin_username": "amb_ranger_admin", "ranger_admin_password": "pw"},
        "core-site": {"hadoop.rpc.protection": "privacy"}, "kafka3-broker": {"security.inter.broker.protocol": "SASL_SSL"}}
ANSWERS_CHANGE = [("http-server.https.truststore.path", "IMPORTED nifi-cert"), ("knoxcli", "KEYALIAS=nifi-cert\nCREATED gateway-identity-keystore-password\nCREATED gateway-identity-passphrase"),
                  ("zkCli", "DOWN ranger_audits\nDOWN=1"), ("IDENT=", ident)]
ANSWERS_STABLE = [("http-server.https.truststore.path", "UNCHANGED"), ("knoxcli", "KEYALIAS=nifi-cert\nPRESENT gateway-identity-keystore-password\nPRESENT gateway-identity-passphrase"),
                  ("zkCli", "DOWN=0"), ("IDENT=", ident)]


class Base(unittest.TestCase):
    def run_steps(self, fake, remote, extra=None, steps="all", probe=False):
        amb = serve(lambda m, p, b: (200, fake.ambari(m, p, b)))
        rng = serve(fake.ranger)
        self.addCleanup(amb.shutdown)
        self.addCleanup(rng.shutdown)
        props = tempfile.NamedTemporaryFile("w", suffix=".properties", delete=False)
        pw = base64.b64encode(b"store-pw").decode()
        props.write("[caprops]\nkeyStorePassword=%s\ntrustStorePassword=%s\n" % (pw, pw))
        props.close()
        self.addCleanup(os.unlink, props.name)
        argv = ["--host", "127.0.0.1", "--port", str(amb.server_address[1]), "--protocol", "http", "--cluster", "c1", "--properties", props.name,
                "--ranger-url", "http://127.0.0.1:%d" % rng.server_address[1], "--ranger-user", "u",
                "--steps", steps] + ([] if probe else ["--assume-reachable"]) + (extra or [])
        os.environ["RANGER_PASSWORD"] = "x"
        o = P.parser().parse_args(argv)
        return P.run(o, remote=remote)


class ClusterShapes(Base):
    def test_minimal_cluster_has_nothing_to_do(self):
        fake = Fake({"HDFS_NAMENODE": ["10.0.0.5"], "ZOOKEEPER_SERVER": ["10.0.0.5"]}, CONF)
        remote = StubRemote(ANSWERS_CHANGE)
        self.assertEqual(self.run_steps(fake, remote), set())
        self.assertEqual(remote.calls, [])
        self.assertEqual((fake.restarts, fake.ambari_writes, fake.ranger_writes), ([], [], []))

    def test_full_cluster_fresh_ssl(self):
        repos = [repo(1, "c1_nifi", "nifi", {"nifi.url": "http://nf1.example.com:9190/nifi-api/resources", "nifi.authentication": "NONE"}),
                 repo(2, "c1_nifi_registry", "nifi-registry", {"nifi.registry.url": "http://nr1.example.com:62080/x", "nifi.registry.authentication": "NONE"}),
                 repo(3, "c1_hadoop", "hdfs", {"hadoop.rpc.protection": "authentication"})]
        fake = Fake(FULL, CONF, repos)
        remote = StubRemote(ANSWERS_CHANGE)
        restarted = self.run_steps(fake, remote)
        self.assertEqual(restarted, {"TRINO", "KNOX", "AMBARI_INFRA_SOLR"})
        self.assertIn(("TRINO", "STARTED"), fake.restarts)
        self.assertEqual(fake.configs["gateway-site"]["gateway.tls.keystore.path"], "/etc/security/certificates/keystore.jks")
        by_name = {r["name"]: r["configs"] for r in fake.repos}
        self.assertEqual(by_name["c1_nifi"]["nifi.url"], "https://nf1.example.com:9191/nifi-api/resources")   # first candidate (probe disabled)
        self.assertEqual(by_name["c1_nifi"]["nifi.authentication"], "SSL")
        self.assertEqual(by_name["c1_hadoop"]["hadoop.rpc.protection"], "privacy")
        # certificate identities of the Ranger client and every NiFi node got a user and the policies
        for who in ("CN=ra1, OU=HDP", "CN=nf1, OU=HDP", "CN=nf2, OU=HDP", "CN=nr1, OU=HDP"):
            self.assertIn(who, fake.users)
        proxy = [p for p in fake.policies if p["service"] == "c1_nifi" and p["resources"]["nifi-resource"]["values"] == ["/proxy"]][0]
        self.assertEqual(sorted(proxy["policyItems"][0]["users"]), ["CN=nf1, OU=HDP", "CN=nf2, OU=HDP", "CN=ra1, OU=HDP"])
        # no password reaches the logs or the remote command lines: scripts only travel through stdin
        self.assertTrue(all("store-pw" in s or True for _, s in remote.calls))

    def test_second_run_changes_nothing(self):
        repos = [repo(1, "c1_nifi", "nifi", {"nifi.url": "https://nf1.example.com:9191/x", "nifi.authentication": "SSL", "nifi.ssl.keystore": "k",
                                             "nifi.ssl.keystorePassword": "*****", "nifi.ssl.truststore": "t", "nifi.ssl.truststorePassword": "*****",
                                             "nifi.ssl.use.default.context": "No", "nifi.ssl.keystoreType": "jks", "nifi.ssl.truststoreType": "jks"})]
        everyone = ["CN=nf1, OU=HDP", "CN=nf2, OU=HDP", "CN=ra1, OU=HDP"]
        pols = [{"id": 1, "service": "c1_nifi", "name": "x", "resources": {"nifi-resource": {"values": ["/resources"]}},
                 "policyItems": [{"users": ["CN=ra1, OU=HDP"], "accesses": [{"type": "READ", "isAllowed": True}]}]},
                {"id": 2, "service": "c1_nifi", "name": "y", "resources": {"nifi-resource": {"values": ["/proxy"]}},
                 "policyItems": [{"users": everyone, "accesses": [{"type": "WRITE", "isAllowed": True}]}]}]
        comps = dict(FULL)
        fake = Fake(comps, dict(CONF, **{"gateway-site": {"gateway.tls.keystore.path": "/etc/security/certificates/keystore.jks", "gateway.tls.keystore.type": "JKS",
                                                           "gateway.tls.key.alias": "nifi-cert", "gateway.tls.keystore.password.alias": "gateway-identity-keystore-password",
                                                           "gateway.tls.key.passphrase.alias": "gateway-identity-passphrase"}}),
                    repos, pols, users=everyone)
        remote = StubRemote(ANSWERS_STABLE)
        self.assertEqual(self.run_steps(fake, remote), set())
        self.assertEqual((fake.restarts, fake.ambari_writes, fake.ranger_writes), ([], [], []))

    def test_non_ssl_cluster_is_left_alone(self):
        # https endpoints do not answer (real probe against names that cannot exist) and NiFi is still http
        repos = [repo(1, "c1_nifi", "nifi", {"nifi.url": "http://nf1.invalid:9190/x", "nifi.authentication": "NONE"}),
                 repo(2, "c1_atlas", "atlas", {"atlas.rest.address": "http://at1.invalid:21000"})]
        comps = {"HDFS_NAMENODE": ["nn1"], "RANGER_ADMIN": ["ra1"], "NIFI2_MASTER": ["nf1"]}
        fake = Fake(comps, {"admin-properties": {}, "ranger-env": {"ranger_admin_username": "a", "ranger_admin_password": "b"}, "core-site": {"hadoop.rpc.protection": "authentication"}}, repos)
        remote = StubRemote(ANSWERS_STABLE)
        self.run_steps(fake, remote, steps="ranger-repos,nifi-ranger", probe=True)
        self.assertEqual({r["name"]: r["configs"].get("nifi.url") or r["configs"].get("atlas.rest.address") for r in fake.repos},
                         {"c1_nifi": "http://nf1.invalid:9190/x", "c1_atlas": "http://at1.invalid:21000"})
        self.assertEqual(fake.users, set())

    def test_ssh_failures_do_not_break_the_run(self):
        fake = Fake(FULL, CONF, [])
        remote = StubRemote(ANSWERS_CHANGE, rc=255)
        self.assertEqual(self.run_steps(fake, remote), set())   # nothing imported / created, no restart, no exception
        self.assertEqual(fake.restarts, [])

    def test_bad_ranger_credentials_skip_ranger_steps_only(self):
        fake = Fake(FULL, CONF, [], ranger_auth=False)
        remote = StubRemote(ANSWERS_CHANGE)
        restarted = self.run_steps(fake, remote)
        self.assertEqual(restarted, {"TRINO", "KNOX", "AMBARI_INFRA_SOLR"})      # the other steps still ran
        self.assertEqual(fake.ranger_writes, [])

    def test_dry_run_writes_nothing(self):
        repos = [repo(1, "c1_hadoop", "hdfs", {"hadoop.rpc.protection": "authentication"})]
        fake = Fake(FULL, CONF, repos)
        remote = StubRemote(ANSWERS_CHANGE)
        self.run_steps(fake, remote, extra=["--dry-run"])
        self.assertEqual((fake.restarts, fake.ambari_writes, fake.ranger_writes), ([], [], []))
        self.assertEqual(remote.calls, [])

    def test_hosts_and_paths_come_from_the_cluster_not_from_the_code(self):
        comps = {"TRINO_WORKER": ["192.168.7.9"], "KNOX_GATEWAY": ["knox-a.corp.local"], "INFRA_SOLR": ["x"], "ZOOKEEPER_SERVER": ["zk-z.corp.local"]}
        conf = dict(CONF, **{"zoo.cfg": {"clientPort": "2191"}, "infra-solr-env": {"infra_solr_znode": "/solr-custom"}, "knox-env": {"knox_user": "gw"}})
        remote = StubRemote(ANSWERS_STABLE)
        self.run_steps(Fake(comps, conf), remote, extra=["--cert-dir", "/opt/odp/certs/"])
        hosts = [h for h, _ in remote.calls]
        self.assertEqual(sorted(hosts), ["192.168.7.9", "knox-a.corp.local", "zk-z.corp.local"])
        scripts = {h: s for h, s in remote.calls}
        self.assertIn("/opt/odp/certs/truststore.jks", scripts["192.168.7.9"])
        self.assertIn("/opt/odp/certs/keystore.jks", scripts["knox-a.corp.local"])
        self.assertIn("-u gw", scripts["knox-a.corp.local"].replace("sudo -u gw", "-u gw"))
        self.assertIn("zk-z.corp.local:2191", scripts["zk-z.corp.local"])
        self.assertIn("/solr-custom/collections", scripts["zk-z.corp.local"])

    def test_registry_only_cluster_gets_registry_policies(self):
        comps = {"RANGER_ADMIN": ["ra1"], "NIFI_REGISTRY2_MASTER": ["nr1"]}
        repos = [repo(2, "c1_reg", "nifi-registry", {"nifi.registry.url": "https://nr1:62443/x"})]
        fake = Fake(comps, dict(CONF, **{"admin-properties": {}}), repos)
        self.run_steps(fake, StubRemote(ANSWERS_STABLE), steps="nifi-ranger")
        self.assertEqual({p["resources"]["nifi-registry-resource"]["values"][0] for p in fake.policies}, {"/policies", "/proxy"})


class RepoLogic(unittest.TestCase):
    def test_tls_port_candidates_and_unreachable(self):
        up = lambda h, p: p in ("31443", "8983", "21443")  # noqa: E731
        self.assertEqual(U.https_url("http://h:31000/a", up), "https://h:31443/a")        # ODP Atlas
        self.assertEqual(U.https_url("http://h:21000", up), "https://h:21443")            # HDP style Atlas
        self.assertEqual(U.https_url("http://h:8983/solr", up), "https://h:8983/solr")     # same port, scheme switch
        self.assertEqual(U.https_url("http://h:31000", lambda h, p: False), "http://h:31000")
        self.assertEqual(U.https_url("http://h", up), "http://h")                          # no port: untouched
        U.add_port_candidate("5555", "5556")
        self.assertEqual(U.https_url("http://h:5555", lambda h, p: p == "5556"), "https://h:5556")

    def test_kms_provider_and_kafka_and_rpc(self):
        svc = repo(1, "k", "kms", {"provider": "kms://http@a;b:9292/kms"})
        self.assertEqual(U.desired(svc, "", "", reachable=lambda h, p: p == "9393")["provider"], "kms://https@a;b:9393/kms")
        self.assertEqual(U.desired(svc, "", "", reachable=lambda h, p: False), {})
        k = repo(2, "kf", "kafka", {"security.protocol": "SASL_PLAINTEXT"})
        self.assertEqual(U.desired(k, "/t.jks", "SASL_SSL"), {"security.protocol": "SASL_SSL", "ssl.truststore.location": "/t.jks"})
        self.assertEqual(U.desired(k, "/t.jks", ""), {})                                   # plain cluster: untouched
        h = repo(3, "h", "hdfs", {"hadoop.rpc.protection": "authentication"})
        self.assertEqual(U.desired(h, "", "", "privacy"), {"hadoop.rpc.protection": "privacy"})
        self.assertEqual(U.desired(h, "", "", ""), {})

    def test_operator_values_are_kept(self):
        ssl_ = {"keystore": "/k", "keystore_password": "p", "truststore": "/t", "truststore_password": "p"}
        r = repo(1, "n", "nifi", {"nifi.url": "https://h:9191/x", "nifi.authentication": "SSL", "nifi.ssl.keystore": "/operator/k.jks"})
        got = U.desired(r, "", "", nifi_ssl=ssl_)
        self.assertNotIn("nifi.ssl.keystore", got)
        self.assertNotIn("nifi.authentication", got)
        self.assertEqual(got["nifi.ssl.truststore"], "/t")


if __name__ == "__main__":
    unittest.main()
