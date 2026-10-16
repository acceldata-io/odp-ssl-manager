#!/usr/bin/env python3
"""Move Ranger repository (service) configs to the HTTPS / TLS endpoints after SSL was enabled.

Ranger creates the repo config once, when the plugin is first set up. Turning SSL on later never touches it, so
Test Connection and resource autocomplete keep pointing at the old http:// endpoints (and kafka keeps
security.protocol=SASL_PLAINTEXT). Plugin policy download is not affected, only the Ranger Admin lookups.

Default is a dry run (read only). Use --apply to write.

  update_ranger_repos.py --ranger https://rhel93111:6182 --user admin --password-env RANGER_PW
  update_ranger_repos.py ... --apply
"""
import argparse
import base64
import json
import os
import re
import socket
import ssl
import sys
import urllib.error
import urllib.request

# plain port -> candidate TLS ports of the service web endpoints Ranger Admin calls. A repo is only moved to https when
# one of these (or the same port, for services that switch scheme in place) really answers TLS, so a cluster without SSL,
# or one that uses other ports, is left alone.
PORT_CANDIDATES = {
    "31000": ["31443"],            # Atlas (ODP)
    "21000": ["21443"],            # Atlas (HDP style)
    "8088": ["8090"],              # YARN ResourceManager
    "9292": ["9393"],              # Ranger KMS
    "7788": ["7790"],              # Schema Registry
    "62080": ["62443"],            # NiFi Registry
    "9190": ["9191", "9443"],      # NiFi 2
    "6080": ["6182"],              # Ranger Admin
    "9874": ["9875"],              # Ozone Manager UI
}
# config keys that hold an endpoint URL
URL_KEYS = ["atlas.rest.address", "yarn.url", "solr.url", "schema.registry.url", "nifi.url", "nifi.registry.url"]
KMS_PROVIDER = re.compile(r"^kms://http@(.+?):(\d+)/kms$")
# NiFi / NiFi Registry repos need a client keystore for the mutual TLS lookup: key prefix per Ranger service type
NIFI_TYPES = {"nifi": "nifi.", "nifi-registry": "nifi.registry."}
_extra_candidates = {}


def add_port_candidate(plain, tls):
    _extra_candidates.setdefault(str(plain), []).append(str(tls))


def tls_reachable(host, port, timeout=4):
    """True when host:port completes a TLS handshake (certificate is not validated, this is a liveness probe)."""
    try:
        ctx = ssl._create_unverified_context()
        with socket.create_connection((host, int(port)), timeout=timeout) as raw:
            with ctx.wrap_socket(raw, server_hostname=host):
                return True
    except Exception:
        return False


def _tls_port(host, port, reachable):
    for cand in _extra_candidates.get(port, []) + PORT_CANDIDATES.get(port, []) + [port]:
        if reachable(host, cand):
            return cand
    return None


def https_url(value, reachable=tls_reachable):
    """http://host:port/path -> https://host:tlsport/path when the TLS endpoint answers, else the value unchanged."""
    m = re.match(r"^http://([^/:]+)(?::(\d+))?(.*)$", value)
    if not m:
        return value
    host, port, rest = m.groups()
    if port is None:
        return value
    tls = _tls_port(host, port, reachable)
    return "https://%s:%s%s" % (host, tls, rest) if tls else value


def desired(service, truststore, kafka_protocol, rpc_protection="", nifi_ssl=None, reachable=tls_reachable):
    """Return {key: new value} for one Ranger service."""
    cfg, new = service.get("configs", {}), {}
    for key in URL_KEYS:
        v = cfg.get(key)
        if v and v.startswith("http://"):
            u = https_url(v, reachable)
            if u != v:
                new[key] = u
    m = KMS_PROVIDER.match(cfg.get("provider", ""))
    if m:
        hosts, port = m.groups()
        tls = _tls_port(hosts.split(";")[0], port, reachable)
        if tls:
            new["provider"] = "kms://https@%s:%s/kms" % (hosts, tls)
    # repos that carry hadoop.rpc.protection (hdfs, hbase, ozone ...) must match the cluster, otherwise the lookup fails
    # with "SaslException: No common protection layer between client and server"
    if rpc_protection and "hadoop.rpc.protection" in cfg and cfg["hadoop.rpc.protection"] != rpc_protection:
        new["hadoop.rpc.protection"] = rpc_protection
    if service.get("type") == "kafka" and kafka_protocol and cfg.get("security.protocol") != kafka_protocol:
        new["security.protocol"] = kafka_protocol
        if kafka_protocol in ("SSL", "SASL_SSL") and truststore:
            new["ssl.truststore.location"] = truststore
    # NiFi / NiFi Registry over https: Ranger Admin calls them with a client certificate; give the repo the keystore and
    # truststore (paths on the Ranger Admin host). Values an operator already set are kept.
    prefix = NIFI_TYPES.get(service.get("type"))
    url = new.get("nifi.url") or new.get("nifi.registry.url") or cfg.get("nifi.url") or cfg.get("nifi.registry.url") or ""
    if prefix and nifi_ssl and url.startswith("https://"):
        wanted = {prefix + "authentication": "SSL", prefix + "ssl.use.default.context": "No",
                  prefix + "ssl.keystore": nifi_ssl.get("keystore"), prefix + "ssl.keystoreType": "jks",
                  prefix + "ssl.keystorePassword": nifi_ssl.get("keystore_password"),
                  prefix + "ssl.truststore": nifi_ssl.get("truststore"), prefix + "ssl.truststoreType": "jks",
                  prefix + "ssl.truststorePassword": nifi_ssl.get("truststore_password")}
        for k, v in wanted.items():
            cur = cfg.get(k)
            if v and (not cur or (k == prefix + "authentication" and cur == "NONE")):
                new[k] = v
    return {k: v for k, v in new.items() if cfg.get(k) != v}


def sync_repos(ranger, user, password, kafka_protocol="", truststore="", rpc_protection="", explicit=None, apply=False, out=print,
               nifi_ssl=None, reachable=tls_reachable):
    """Compare (and with apply=True fix) every Ranger repo. Returns the number of repositories that differ."""
    ctx = ssl._create_unverified_context()
    hdr = {"Authorization": "Basic " + base64.b64encode(("%s:%s" % (user, password)).encode()).decode(),
           "Accept": "application/json", "Content-Type": "application/json"}

    def call(method, path, body=None):
        req = urllib.request.Request(ranger.rstrip("/") + path, method=method, headers=hdr,
                                     data=json.dumps(body).encode() if body is not None else None)
        return json.loads(urllib.request.urlopen(req, context=ctx, timeout=60).read() or b"{}")

    explicit = explicit or {}
    changed = 0
    for svc in sorted(call("GET", "/service/public/v2/api/service"), key=lambda s: s["name"]):
        if svc["name"].endswith("_tag"):
            continue
        new = desired(svc, truststore, kafka_protocol, rpc_protection, nifi_ssl, reachable)
        new.update({k: v for k, v in explicit.get(svc["name"], {}).items() if svc.get("configs", {}).get(k) != v})
        if not new:
            continue
        changed += 1
        for k, v in new.items():
            out("%-26s %-24s %s -> %s" % (svc["name"], k, svc["configs"].get(k), v))
        if apply:
            svc["configs"].update(new)       # password fields come back masked and are kept by Ranger
            try:
                call("PUT", "/service/public/v2/api/service/%s" % svc["id"], svc)
                out("%-26s updated" % svc["name"])
            except urllib.error.HTTPError as e:
                out("%-26s FAILED %s %s" % (svc["name"], e.code, e.read().decode()[:200]))
    return changed


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ranger", required=True, help="Ranger Admin URL, e.g. https://host:6182")
    ap.add_argument("--user", default="admin")
    ap.add_argument("--password-env", default="RANGER_PASSWORD", help="env var holding the Ranger admin password")
    ap.add_argument("--kafka-protocol", default="", help="security.protocol to set on kafka repos (SASL_SSL, SSL)")
    ap.add_argument("--truststore", default="/etc/security/certificates/truststore.jks",
                    help="truststore path on the Ranger Admin host, used for kafka SSL lookups")
    ap.add_argument("--set", action="append", default=[], metavar="REPO:KEY=VALUE",
                    help="set one repo config key to an exact value, e.g. rhel9311_hadoop:hadoop.rpc.protection=privacy "
                         "(repeatable); use it for settings that must match the cluster, like the HDFS RPC protection")
    ap.add_argument("--rpc-protection", default="", help="hadoop.rpc.protection of the cluster (core-site); repos that carry the key are moved to it")
    ap.add_argument("--port-map", action="append", default=[], metavar="PLAIN=TLS",
                    help="extra plain->TLS port candidate for the endpoint check, e.g. 21000=21443 (repeatable)")
    ap.add_argument("--assume-reachable", action="store_true",
                    help="do not probe the https endpoints (use when this host cannot reach the cluster hosts)")
    ap.add_argument("--apply", action="store_true", help="write the changes (default is a dry run)")
    a = ap.parse_args()

    pw = os.environ.get(a.password_env)
    if not pw:
        sys.exit("set %s to the Ranger admin password" % a.password_env)
    explicit = {}
    for item in a.set:
        repo, _, kv = item.partition(":")
        key, _, value = kv.partition("=")
        if not repo or not key:
            sys.exit("--set needs REPO:KEY=VALUE, got %r" % item)
        explicit.setdefault(repo, {})[key] = value
    for item in a.port_map:
        plain, _, tls = item.partition("=")
        add_port_candidate(plain, tls)
    changed = sync_repos(a.ranger, a.user, pw, a.kafka_protocol, a.truststore, a.rpc_protection, explicit, a.apply,
                         reachable=(lambda h, p: True) if a.assume_reachable else tls_reachable)
    print("%d repositories %s" % (changed, "updated" if a.apply else "would change (dry run, use --apply)"))


if __name__ == "__main__":
    main()
