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
import ssl
import sys
import urllib.error
import urllib.request

# plain port -> TLS port of the service web endpoints Ranger Admin calls
PORT_MAP = {
    "31000": "31443",   # Atlas
    "8088": "8090",     # YARN ResourceManager
    "9292": "9393",     # Ranger KMS
    "7788": "7790",     # Schema Registry
    "62080": "62443",   # NiFi Registry
    "9190": "9191",     # NiFi 2
    "6080": "6182",     # Ranger Admin
    "9874": "9875",     # Ozone Manager UI
}
# config keys that hold an endpoint URL and the keys that carry the scheme inside the value
URL_KEYS = ["atlas.rest.address", "yarn.url", "solr.url", "schema.registry.url", "nifi.url", "nifi.registry.url"]
KMS_PROVIDER = re.compile(r"^kms://http@(.+?):(\d+)/kms$")


def https_url(value):
    m = re.match(r"^http://([^/:]+)(?::(\d+))?(.*)$", value)
    if not m:
        return value
    host, port, rest = m.groups()
    if port is None:
        return value
    return "https://%s:%s%s" % (host, PORT_MAP.get(port, port), rest)


def desired(service, truststore, kafka_protocol):
    """Return {key: new value} for one Ranger service."""
    cfg, new = service.get("configs", {}), {}
    for key in URL_KEYS:
        v = cfg.get(key)
        if v and v.startswith("http://"):
            new[key] = https_url(v)
    m = KMS_PROVIDER.match(cfg.get("provider", ""))
    if m:
        hosts, port = m.groups()
        new["provider"] = "kms://https@%s:%s/kms" % (hosts, PORT_MAP.get(port, port))
    if service.get("type") == "kafka" and kafka_protocol and cfg.get("security.protocol") != kafka_protocol:
        new["security.protocol"] = kafka_protocol
        if kafka_protocol in ("SSL", "SASL_SSL") and truststore:
            new["ssl.truststore.location"] = truststore
    return {k: v for k, v in new.items() if cfg.get(k) != v}


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
    ap.add_argument("--apply", action="store_true", help="write the changes (default is a dry run)")
    a = ap.parse_args()

    pw = os.environ.get(a.password_env)
    if not pw:
        sys.exit("set %s to the Ranger admin password" % a.password_env)
    ctx = ssl._create_unverified_context()
    hdr = {"Authorization": "Basic " + base64.b64encode(("%s:%s" % (a.user, pw)).encode()).decode(),
           "Accept": "application/json", "Content-Type": "application/json"}

    def call(method, path, body=None):
        req = urllib.request.Request(a.ranger.rstrip("/") + path, method=method, headers=hdr,
                                     data=json.dumps(body).encode() if body is not None else None)
        return json.loads(urllib.request.urlopen(req, context=ctx, timeout=60).read() or b"{}")

    explicit = {}
    for item in a.set:
        repo, _, kv = item.partition(":")
        key, _, value = kv.partition("=")
        if not repo or not key:
            sys.exit("--set needs REPO:KEY=VALUE, got %r" % item)
        explicit.setdefault(repo, {})[key] = value

    changed = 0
    for svc in sorted(call("GET", "/service/public/v2/api/service"), key=lambda s: s["name"]):
        if svc["name"].endswith("_tag"):
            continue
        new = desired(svc, a.truststore, a.kafka_protocol)
        new.update({k: v for k, v in explicit.get(svc["name"], {}).items() if svc.get("configs", {}).get(k) != v})
        if not new:
            continue
        changed += 1
        for k, v in new.items():
            print("%-26s %-24s %s -> %s" % (svc["name"], k, svc["configs"].get(k), v))
        if a.apply:
            svc["configs"].update(new)       # password fields come back masked and are kept by Ranger
            try:
                call("PUT", "/service/public/v2/api/service/%s" % svc["id"], svc)
                print("%-26s updated" % svc["name"])
            except urllib.error.HTTPError as e:
                print("%-26s FAILED %s %s" % (svc["name"], e.code, e.read().decode()[:200]))
    print("%d repositories %s" % (changed, "updated" if a.apply else "would change (dry run, use --apply)"))


if __name__ == "__main__":
    main()
