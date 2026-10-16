# TLS enablement runbook (ODP 3.4, Ambari, Kerberos or not)

Ticket: ODP-8345 (alerts, Kafka Connect/MirrorMaker, Livy) and the ODP-8311 ssl_manager branch.
Reference cluster: rhel9311 (3 hosts, Ambari 3, Kerberos ADSRE.COM, NameNode HA, Ranger HTTPS only).

## 1. How SSL is turned on

1. Jenkins job `SSL_Setup` downloads `ssl_setup.yml` from `mirror.odp.acceldata.dev/scripts` and runs it on the
   Ambari host with `SERVICES=all UI=all GIT_BRANCH=<odp-ssl-manager branch>`.
2. The playbook runs `complete_automated_ssl_setup_hetzner.sh`, which clones `odp-ssl-manager` at `GIT_BRANCH`
   and calls `bin/ssl_manager.py --ca --enable-ssl --service=all --ui=all`.
3. The CA is `HDPCA`. Files end up in `/etc/security/certificates` on every host:
   `keystore.jks`, `truststore.jks`, `keystore.p12`, `cert.pem`, `key.pem` (unencrypted PEM, alias `nifi-cert`).
4. `ssl_manager.py` reads `conf/enable_configs2700.yaml`, replaces the placeholders
   (`$keystore $truststore $keystorepassword $truststorepassword $pemCert $pemKey $p12_location $rangerPolicyUrl ...`)
   and writes the result to Ambari config types. It only touches services that are installed
   (`ALL_SERVICES`/`ALL_UI` are intersected with the installed list), so a new entry is safe on clusters without the service.
5. After the run, restart the changed services (the tool does not).

To use these changes: merge the commits into the branch the job passes as `GIT_BRANCH` (ODP-8311). No Jenkins or
playbook edit is needed, the job already passes `all`.

To undo: `--disable-ssl` uses `conf/disable_configs2700.yaml` (entries were added for the new services).

## 2. What was added in this round

| Service | Config type | Effect |
|---|---|---|
| Livy3 (SPARK3, SPARK3_x, LIVY3) | `livy3*-conf` | `livy.ssl=true`. Without it Livy kept serving HTTP on the SSL port and the Spark3 Livy alert went CRITICAL |
| Spark4 history server + Livy4 | `spark4-defaults`, `livy4-conf` | `spark.ssl.*`, `spark.ui.https.enabled`, `livy.ssl=true`, keystores |
| Hive Server2 web UI (10002) | `hive-site` | `hive.server2.webui.use.ssl` + keystore |
| Spark3 Thrift server (10016) | `spark3-hive-site-override` | `hive.server2.use.SSL` + keystore |
| Airflow3 web | `airflow3-webserver-site` | `enable_ssl`, PEM cert/key |
| Hue | `hue-desktop-site`, `hue-hive-site` | PEM cert/key, CA bundle, `ssl_password` (see 4.2) |
| MLflow | `mlflow-conf` | new mpack options `mlflow.ssl.enabled/certfile/keyfile` |
| ClickHouse | `clickhouse-env` | `enable_ssl`, cert, key, CA (the old `clickhouse-application` type does not exist in this stack) |
| Trino | `trino-env` | HDPCA keystore instead of the per-host self-signed `/etc/trino/trino.jks` |
| Ranger plugins Atlas, Knox, Trino | `ranger-*-security`, `ranger-trino-policymgr-ssl` | policy URL to `https://<ranger>:6182`, Trino truststore. Before this they kept polling the plain URL and stayed on an old policy version |

`ssl_manager.py` change: a config type that is missing in the stack is skipped with a warning instead of aborting the
rest of that service.

## 3. mpack changes (ambari-mpacks, branch ODP-8345)

* **Kafka3 / Kafka4 Connect and MirrorMaker2.** When the brokers listen on SSL or SASL_SSL, the generated
  Connect and MM2 configs follow the broker: security protocol and truststore *location* are copied, the keystore only
  if the broker requires client auth. Values set by the operator are kept, blank `ssl.*` entries are removed,
  PLAINTEXT/SASL_PLAINTEXT brokers get exactly the old output. Passwords are not copied (they only exist as env vars if
  the stored env content exports them; copying gave "Password verification failed").
* **Alerts.** `atlas_server_process` and `seatunnel_master_webui_port` are WEB alerts using the service HTTPS property.
  Ambari does not refresh existing alert definitions on upgrade. For a running cluster update them once:
  `PUT /api/v1/clusters/<c>/alert_definitions/<id>` with the new source (Atlas id 12, SeaTunnel id 172 on rhel9311).
* **MLflow.** `mlflow server --uvicorn-opts "--ssl-certfile .. --ssl-keyfile .."` when enabled, CORS origins follow the
  scheme, the service check probes https, the quick link switches to https. Default is off.

Ambari reads quick links and stack definitions at server start. After deploying an mpack file by hand, restart
`ambari-server` (this also clears the temporary KDC admin credential, re-add it before Kerberos operations).

## 3a. Kafka3 / Kafka4, Kafka Connect, MirrorMaker2 (step by step)

1. **Brokers.** `SSL_Setup` sets, in `kafka3-broker` / `kraft4-broker`: `security.inter.broker.protocol=SASL_SSL` (SSL
   when the cluster is not Kerberos), `ssl.keystore.location/password`, `ssl.key.password`,
   `ssl.truststore.location/password`. Listener ports on rhel9311: Kafka3 6669, Kafka4 7001.
2. **Restart the brokers** (rolling), then check:
   `kafka-broker-api-versions.sh --bootstrap-server <host>:6669 --command-config client.properties`
   with `security.protocol=SASL_SSL`, `sasl.mechanism=GSSAPI`, `sasl.kerberos.service.name=kafka`,
   `ssl.truststore.location=/etc/security/certificates/truststore.jks` (no password needed to read certificates).
3. **Connect (8083 / 8084) and MirrorMaker2.** Nothing to set by hand. With the ODP-8345 mpack the generated
   `connect-distributed.properties` / `connect-mirror-maker2.properties` copy the broker protocol and the truststore
   *location* when the brokers use SSL/SASL_SSL (keystore only if the broker requires client auth, passwords are
   not copied). Blank `ssl.*` lines are removed. Restart Connect and MirrorMaker2 after the brokers.
   Check: `curl http://<host>:8083/` (Kafka3) and `:8084` (Kafka4) return 200; MirrorMaker2 log shows
   offsets being committed (`WorkerSourceTask{id=MirrorHeartbeatConnector-0} Committing offsets`).
4. **Ranger Kafka plugin.** The plugin and its `ranger-kafka*-policymgr-ssl` come from the SSL tool. The Ranger
   *repository* (Test Connection) is separate, see section 4a.
5. If MirrorMaker2 logs `terminated during authentication`/brokers log `SSL handshake failed` right after a
   restart, wait: they stop once the brokers are back. A persistent failure means the truststore does not
   contain HDPCA or the bootstrap list points at the wrong port.
6. Ambari sometimes reports a component INSTALLED although the JVM runs (stale PID file) or leaves Connect stopped
   after a Kafka restart; start it from Ambari and check the REST port.

## 4. Manual steps for an existing cluster (what was done on rhel9311)

Use the Ambari UI, or `configs.py` / the REST API, then restart the service.

### 4.1 Generic pattern
Keystore `/etc/security/certificates/keystore.jks`, truststore `.../truststore.jks`, PEM `.../cert.pem` + `.../key.pem`.
Password is the keystore password from `ca.properties` (do not write it into tickets).

### 4.2 Hue
`hue-desktop-site`: `ssl_enable=true`, `ssl_certificate=<cert.pem>`, `ssl_private_key=<key.pem>`,
`ssl_cacerts=<cert.pem>`, **`ssl_password=<keystore password>`**.
`hue-hive-site`: `ssl_cacerts=<cert.pem>`.
Why `ssl_password` matters: the Hue mpack writes `ssl_password_script` (a credential store lookup of alias `ssl`) into
hue.ini. If no password is configured the alias does not exist, the lookup fails and `runcpserver` exits silently
with code 0 (no listener on 8888, nothing in error.log). Setting `ssl_password` makes the mpack create the alias.
Check: `curl -k -o /dev/null -w '%{http_code}' https://<hue-host>:8888/` returns 302.

### 4.3 MLflow
`mlflow-conf`: `mlflow.ssl.enabled=true`, `mlflow.ssl.certfile`, `mlflow.ssl.keyfile`. Restart MLflow.
MLflow rejects any Host header that is not in `mlflow.allowed.hosts` (`Invalid Host header - possible DNS rebinding
attack detected`). The mpack now always adds the host's own short name, FQDN and IP (`<name>:5000`) to the allowed hosts
and `https://<name>:5000` to the CORS origins, so the UI opens by name or by IP. Names behind a load balancer or Knox
must be added to `mlflow.allowed.hosts` by hand. After changing the mpack file on a live cluster restart
`ambari-server` so the quick link (`https://<host>:5000/`) is re-read, then hard-refresh the Ambari page.

### 4.4 Spark4 / Livy4, Airflow3, HS2 UI, Spark3 thrift, ClickHouse, Trino
Set the properties from the table in section 2, restart the service.

### 4.5 Ranger plugins that were stuck (Atlas, Knox, Trino)
Set `ranger.plugin.<svc>.policy.rest.url` to `https://<ranger-host>:6182`, give the plugin a truststore that has HDPCA
(Knox also needs its keystore, Trino its truststore in the JDK cacerts), restart. Verify in Ranger
Audit > Plugins that the policy version equals the repository version.

### 4a. Ranger repositories (Test Connection) after SSL
Ambari creates a Ranger repository once, at first service start, and never updates it
(`create_ranger_repository` only creates when missing). After SSL the repo still points at http endpoints and Kafka
repos stay `SASL_PLAINTEXT`; plugin policy download is not affected, only Test Connection / autocomplete.
Also `atlas.rest.address` stays `http://...:31000` unless the SSL tool rewrites it (it now does).

```
export RANGER_PASSWORD=<ranger admin password>
bin/update_ranger_repos.py --ranger https://<ranger-host>:6182 --user admin --kafka-protocol SASL_SSL        # dry run
bin/update_ranger_repos.py --ranger https://<ranger-host>:6182 --user admin --kafka-protocol SASL_SSL --apply
bin/update_ranger_repos.py --ranger ... --set <hdfs repo>:hadoop.rpc.protection=privacy --apply      # exact values
```
Port map used: Atlas 31000->31443, YARN 8088->8090, KMS 9292->9393, Registry 7788->7790, NiFi Registry 62080->62443,
NiFi 9190->9191, Solr same port with https. Use `--kafka-protocol SSL` on clusters without Kerberos.
NiFi repos need the NiFi client certificate identity; Test Connection can still fail there after the URL fix.
Tracked in ODP-8349.

**Result on rhel9311 (16 repos):** 14 pass Test Connection after the steps below; NiFi and NiFi Registry connect over
TLS but get `403 No applicable policies` until the identity of the Ranger Admin client certificate
(`CN=<ranger-host>, OU=HDP`) has a policy on `/resources` (NiFi) / `/policies` (NiFi Registry).

Settings that must match the cluster, set with `--set REPO:KEY=VALUE`:
* HDFS repo `hadoop.rpc.protection` = the value of `core-site` `hadoop.rpc.protection` (privacy here). With
  `authentication` the lookup fails with `SaslException: No common protection layer between client and server`.
* NiFi / NiFi Registry repo: `nifi.authentication=SSL`, `nifi.ssl.keystore/keystoreType/keystorePassword/truststore/...`
  (Ranger Admin host paths). Without them the TLS call cannot be made.

### 4a-1. Trino: workers must trust the coordinator certificate
Switching `trino-env` `ssl_keystore` to the HDPCA keystore makes every node present an HDPCA certificate, but Trino
validates with the JDK `cacerts` (`http-server.https.truststore.path`), which has no HDPCA. Symptom: workers log
`Error announcing node state to https://<coordinator>:9098/v1/announce`, the coordinator has no workers
(`node-scheduler.include-coordinator=False`) and every query, including Ranger Test Connection, stays QUEUED.
Fix, on every Trino host, then restart Trino:
`keytool -importcert -noprompt -alias hdpca -file hdpca.pem -keystore /opt/java25/lib/security/cacerts -storepass <cacerts password>`
(`hdpca.pem` is exported from `/etc/security/certificates/truststore.jks`).

### 4a-2. Knox: use the HDPCA certificate instead of the self-signed gateway identity
Ranger Admin cannot validate the self-signed gateway certificate (Test Connection: `javax.net.ssl...`).
Knox 2.1 can read an external keystore. On every Knox host, as the knox user, store the keystore password:
`knoxcli.sh create-alias gateway-identity-keystore-password --value <keystore password>` and the same for
`gateway-identity-passphrase`. In `gateway-site` set `gateway.tls.keystore.path=/etc/security/certificates/keystore.jks`,
`gateway.tls.keystore.type=JKS`, `gateway.tls.key.alias=nifi-cert`,
`gateway.tls.keystore.password.alias=gateway-identity-keystore-password`,
`gateway.tls.key.passphrase.alias=gateway-identity-passphrase`, restart Knox. Check:
`openssl s_client -connect <knox>:8443 | openssl x509 -noout -issuer` shows `CN=HDPCA`.

### 4a-3. Ranger Admin Audit tab and Atlas indexes after SSL (fixed by restarting Infra Solr)
Symptom: Ranger Audit tab `Error running solr query ... Could not find a healthy node to handle the request`.
Cause: after SSL was enabled the single replica of `ranger_audits`, `edge_index`, `vertex_index` and `fulltext_index`
stayed marked `"state":"down"` in ZooKeeper (`/infra-solr/collections/<c>/state.json`) although the node was live and
serving writes. At the first start after SSL the core logged
`There is conflicting information about the leader of shard: shard1 our state says:http://... but zookeeper says:https://...`
and never republished itself as active, so SolrJ clients (Ranger Admin) skip the replica.
Fix: restart Ambari Infra Solr (all hosts) once SSL is on; all replicas turned `active` and the Audit tab works.
Check: `zkCli.sh -server <zk>:2181 get /infra-solr/collections/ranger_audits/state.json` shows `"state":"active"`.
Add this restart to the SSL procedure after `AMBARI_INFRA_SOLR` is switched to https.

### 4a-4. Plugins whose audit writer does not see core-site (Trino, NiFi, NiFi Registry)
Their HDFS audit writer builds its own Hadoop configuration from `xasecure.audit.destination.hdfs.config.*`
(Ambari passes only the HA keys). With `hadoop.rpc.protection=privacy` on the cluster, audits to HDFS fail with
`SaslException: No common protection layer between client and server`. Set in `ranger-trino-audit`,
`ranger-nifi2-audit`, `ranger-nifi-registry2-audit`:
`xasecure.audit.destination.hdfs.config.hadoop.rpc.protection=privacy` and
`xasecure.audit.destination.hdfs.config.dfs.data.transfer.protection=authentication,privacy`, restart the service.
Proper fix: Ambari's audit config generator (`setup_ranger_plugin_xml.py`) should also pass these two keys from core-site/hdfs-site.

### 4a-5. Open: NiFi
* NiFi Ranger plugin audit to Solr never connects: `NoSuchMethodError: CloudSolrClient$Builder.withHttpClient(HttpClient)`
  (SolrJ on the NiFi plugin classpath does not match Ranger's audit library). Not TLS related.
* NiFi repo Test Connection still returns `403 Authentication Proxy Server not trusted` for the Ranger Admin client
  certificate `CN=rhel93111, OU=HDP` even with the read and `/proxy` write policies loaded; cause not found.
  NiFi Registry passes.

### 4b. Things that look wrong but are not
* **Knox plugin on an old policy version after a restart.** It initialises on the first request through the
  `default` topology. One request (`curl -k -u <user> https://<knox>:8443/gateway/default/webhdfs/v1/?op=LISTSTATUS`)
  and it downloads the current version.
* **HBase master with an old policy version.** The standby master does not load the Ranger coprocessor, only the
  active one polls (`curl -k https://<master>:16010/jmx?qry=Hadoop:service=HBase,name=Master,sub=Server | grep isActiveMaster`).

## 5. Verification

```
# every listening non-loopback port, TLS or plain, with issuer
for h in <hosts>; do ssh $h 'bash -s' < tlsprobe.sh; done
curl -k -o /dev/null -w '%{http_code}\n' https://<host>:<port>/       # per UI
ranger Audit > Plugins                                                  # policy version per plugin
Ambari alerts: no CRITICAL; Service Check per service
Kafka: kafka-broker-api-versions --bootstrap-server <h>:6669 --command-config <client.properties>
Connect REST: curl http://<h>:8083/  (3.x)  and :8084 (4.x)
```

Result on rhel9311 after this round: 102 TLS ports (98 signed by HDPCA).

## 5a. Ranger audits after SSL (ODP-8357)

* **HDFS audit writer of Trino, NiFi, NiFi Registry, Schema Registry.** These plugins build their own Hadoop client
  config from `xasecure.audit.destination.hdfs.config.*` and do not see core-site. After SSL the cluster uses
  `hadoop.rpc.protection=privacy`; the writer defaults to `authentication` and fails with
  `SaslException: No common protection layer between client and server`, so no HDFS audit file is written.
  The SSL tool now sets `xasecure.audit.destination.hdfs.config.hadoop.rpc.protection=privacy` and
  `xasecure.audit.destination.hdfs.config.dfs.data.transfer.protection=authentication,privacy` in `ranger-trino-audit`,
  `ranger-nifi2-audit`, `ranger-nifi-registry2-audit` and `ranger-schema-registry-audit` (removed by `--disable-ssl`).
  Check: `hdfs dfs -ls -R /ranger/audit/<trino|nifi|nifi-registry|schema-registry>` shows a file for today.
* **Atlas.** `atlas_client.py` and `atlas_server.py` both write `atlas-application.properties`; the client writer used to
  reset `atlas.authorizer.impl` to the Ambari default `simple`, so Atlas ignored Ranger and no Ranger audits were
  produced (ambari-mpacks, same ticket). Check: `grep authorizer.impl /etc/atlas/conf/atlas-application.properties`
  says `ranger` after a client config refresh.

## 6. Still plain or self-signed (not covered)

| Item | Why / next step |
|---|---|
| ZooKeeper 2181 | 2182 (TLS) exists; closing 2181 needs every client moved first. Decision pending |
| Knox 8443 | TLS, but self-signed. Needs the gateway identity keystore replaced with the HDPCA cert (`gateway.jks` password = master secret) |
| Trino 9097, ClickHouse 8123 | plain listener stays open next to the TLS one; needs a product option to disable it |
| Pinot broker/server/minion, controller 9000 | only the controller has TLS settings in the mpack |
| YARN ATS HBase (17000/17010) | no TLS option in the mpack |
| Kafka3/4 Connect REST (8083/8084) | plain REST; Connect to brokers is TLS |
| Hive Metastore thrift, HDFS/YARN RPC | RPC protocols, protected by Kerberos, not TLS |

## 7. What was and was not tested

* Tested live on rhel9311 (Kerberos **and** SSL): everything in section 2, Kafka3/4 Connect and MM2 running and
  committing, Atlas/SeaTunnel/Livy alerts OK, MLflow service check passes.
* Tested only by rendering/unit matrix: Kafka Connect/MM2 for PLAINTEXT, SASL_PLAINTEXT, SSL, SASL_SSL.
* **Not tested on a real cluster:** SSL without Kerberos, Kerberos without SSL, neither. The new settings are
  guarded (off by default, only applied when the broker/service already uses TLS), but run `SSL_Setup` on a
  non-Kerberos cluster and a fresh install before relying on it.
* Open follow-ups: the Hue/Knox/Pinot/ATS items above, Jenkins `SSL_Setup` job stores an SSH private key as a
  default parameter value (visible in the job config), consider moving it to a credential.
