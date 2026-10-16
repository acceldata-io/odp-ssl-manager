# Ranger audits (HDFS + Solr) and Test Connection - enable and verify (ODP 3.4, Kerberos)

Worked out on the `rhel9py12final` cluster (ODP 3.4.3.0-1027, Kerberos `ADSRE.COM`, Ranger Admin on http/6080, Infra Solr for audit).
Everything below was applied through Ambari config changes plus service restarts. No credentials are written in this document;
use your own Ranger admin, KDC admin and Ambari accounts.

## Current status (rhel9py12final, 2026-10-16 17:50 IST)

"Plugins current" is the number of registered plugins on the latest policy version out of the number registered. Times in brackets are how long ago the newest Solr event or HDFS audit file was written.

| Repo | Test Connection | Plugins current | Solr audit (events, newest) | HDFS audit (newest file) | Note |
|---|---|---|---|---|---|
| atlas | OK | 1/1 | 2 (36m) | atlas 37m | events are denied writes from the probe; allowed reads were not recorded |
| hadoop (HDFS) | OK | 2/2 | 23671 (0m) | hdfs 68m | |
| hbase | OK | 4/5 | 33 (84m) | master 66m, regional 64m | `hbaseMaster` on the first host stays on an older policy version |
| hive | OK | 1/1 | 1 (53m) | hiveServer2 54m | only my probe query; otherwise idle |
| kafka3 | OK | 3/3 | 144 (5m) | kafka3 64m | |
| kafka4 | OK | 3/3 | 144 (5m) | kafka4 64m | |
| kms | OK | 2/2 | 0 | kms 80m | Solr events not shown in the admin audit view for this repo type |
| knox | OK | 1/1 | 4 (55m) | knox 15m | worked after the topology moved to `XASecurePDPKnox` |
| kudu | OK | 1/1 | 2 (35m) | kudu 35m | after enabling both audit destinations |
| nifi | OK | 2/2 | 43 (0m) | nifi 0m | TLS enabled on NiFi 2, plugin registered, audits to Solr and HDFS |
| nifi_registry | OK | 1/1 | 7 (0m) | nifi-registry 2m | TLS enabled, plugin registered, audits to Solr and HDFS |
| ozone | OK | 3/3 | 192 (5m) | ozone 80m | |
| registry (Schema Registry) | OK | 1/1 | 8 (35m) | schema-registry 35m | after fixing the audit config |
| solr | OK | 1/1 | 24 (5m) | solr 30m | HDFS audit enabled in this work |
| trino | OK | 2/2 | 39 (5m) | trino 73m | |
| yarn | OK | 2/2 | 32 (32m) | yarn 33m | |

Summary: 16 of 16 Test Connection pass. Solr and HDFS audit both flow for 16 repos except KMS (HDFS only in the admin view). HBase has one of five plugins on an older policy version.

## 1. What "working" looks like

For every Ranger repo, check four things:

| Check | How | Pass |
|---|---|---|
| Test Connection | `POST /service/plugins/services/validateConfig` with the repo JSON, or the UI button | `statusCode` 0 |
| Plugin registered and on the latest policy | `GET /service/plugins/plugins/info`, compare `policyActiveVersion` with the repo `policyVersion` | every plugin of the repo on the latest version |
| Solr audit | Ranger Admin > Audit > Access, or `GET /service/assets/accessAudit?repoName=<repo>` | events with recent `eventTime` |
| HDFS audit | `hdfs dfs -ls -R /ranger/audit/<appId>` | a file for today, modified recently |

A repo can legitimately show zero audit events when nothing used it. Generate access (section 6) before calling it broken.

## 2. Audit configuration (per service `ranger-<svc>-audit`)

Every plugin must have real endpoints, not the stack placeholders (`hdfs://hostname:8020`, `hdfs://localhost:8020`, `NONE`):

```
xasecure.audit.is.enabled                          = true
xasecure.audit.destination.hdfs                    = true
xasecure.audit.destination.hdfs.dir                = hdfs://<nameservice>/ranger/audit      (HA: nameservice; non-HA: nn-host:8020)
xasecure.audit.destination.solr                    = true
xasecure.audit.destination.solr.zookeepers         = zk1:2181,zk2:2181,zk3:2181/infra-solr
xasecure.audit.destination.solr.force.use.inmemory.jaas.config = true      (Kerberos)
xasecure.audit.jaas.Client.option.serviceName      = solr                   (Kerberos: the SOLR service name, never the plugin's own)
```

On Kerberos clusters also keep the `xasecure.audit.jaas.Client.option.principal/keyTab` pair the Kerberos descriptor creates for the service.

Repos that were wrong on this cluster and what they needed:

| Service (Ambari type) | Found | Set |
|---|---|---|
| `ranger-kudu-audit` | HDFS and Solr destinations false, HDFS dir `hdfs://hostname:8020`, zookeepers `NONE` | both destinations true, real nameservice, real ZK quorum |
| `ranger-schema-registry-audit` | Solr false, HDFS dir `hdfs://hostname:8020`, zookeepers `NONE`, JAAS `serviceName=registry` | Solr true, real HDFS dir and ZK quorum, in-memory JAAS, `serviceName=solr` |
| `ranger-solr-audit` | HDFS false, dir `hdfs://localhost:8020` | HDFS true, real nameservice (Solr auditing itself to Solr stays off) |

After changing an audit config restart the service so the plugin reloads it.

## 3. Enabling a Ranger plugin

Property that turns each plugin on (Ambari type, then property):

| Service | Config type | Property | Extra |
|---|---|---|---|
| HDFS, YARN, HBase, Hive, Knox, Atlas, Ozone | `ranger-<svc>-plugin-properties` | `ranger-<svc>-plugin-enabled = Yes` | mirrored in `ranger-env` |
| Kafka3 / Kafka4 | `ranger-kafka3/4-plugin-properties` | `ranger-kafka-plugin-enabled = yes` | |
| Solr | `ranger-solr-plugin-properties` | `ranger-solr-plugin-enabled = Yes` | also `ranger-env` |
| NiFi 2 | `ranger-nifi2-plugin-properties` | `ranger-nifi-plugin-enabled = Yes` | also `ranger-env`; **needs NiFi TLS** (section 5) |
| NiFi Registry 2 | `ranger-nifi-registry2-plugin-properties` | `ranger-nifi-registry-plugin-enabled = Yes` | also `ranger-env`; **needs TLS** |
| Kudu | `kudu-master-env` | `enable_ranger = true` | |
| Trino | `trino-env` | `enable_ranger = true` | JDK 11+ |
| Schema Registry | `registry-env` | `enable_ranger = true` | |

Restart the service after changing the flag. The repo appears in Ranger Admin when the plugin first connects.

## 4. Test Connection - fixes that were needed

| Repo | Symptom | Cause | Fix |
|---|---|---|---|
| Trino | `Unable to retrieve catalog list`, log shows `StatementClient thread was interrupted` | Trino nodes announce over HTTPS but the JVM does not trust the coordinator's self-signed certificate, so the node never joins and every query stays queued; Ranger's lookup query times out | Import every Trino node's certificate into the cacerts used by the Trino JDK on **every** Trino node (coordinator included), then restart Trino. Watch `trino.log` for `Error announcing node state` |
| Knox | `Unable to retrieve any files using given parameters` | (a) the Knox demo LDAP (port 33389) was not running on the host the topology points at; (b) the repo password did not match the LDAP user; (c) Ranger Admin's own JVM trusted nothing | (a) start it: Ambari custom command `STARTDEMOLDAP` on the host named in `main.ldapRealm.contextFactory.url`; (b) set the repo `password` to the LDAP password (demo LDAP `admin` user); (c) see "Ranger Admin truststore" below |
| every HTTPS lookup to a service with a private-CA or self-signed certificate | Test Connection fails with a generic REST error | `ranger-admin-site` `ranger.truststore.file` pointed at `/etc/ranger/admin/conf/ranger-admin-keystore.jks`, which does not exist. The JVM then runs with an empty truststore | Point `ranger.truststore.file` at a store that contains the CAs/certificates of the services Ranger calls, with matching `ranger.truststore.password` (here the system Java cacerts `/etc/pki/java/cacerts`, after importing the Knox gateway certificates). Restart Ranger Admin. Confirm with `jcmd <ranger pid> VM.system_properties \| grep trustStore` |
| NiFi, NiFi Registry | Test Connection fails or plugin never registers | see section 5 | |

Verification of the Ranger Admin truststore on RHEL: `update-ca-trust` does not extract a plain self-signed (non-CA) leaf certificate into the Java store.
Import the certificate directly with `keytool -importcert -cacerts ...`. A later `update-ca-trust` run replaces that file, so import the CA that signed the service
certificate when you can instead of leaf certificates.

## 5. NiFi and NiFi Registry (need TLS first)

NiFi 2 and NiFi Registry 2 load their Ranger authorizer only when the web server is secured. With `nifi.web.https.port` (or `nifi.registry.web.https.port`) empty
the policy cache stays a 0-byte file, no plugin registers and no audit is written. What was done on this cluster, in order:

1. Generate a CA and per-host keystores with the SSL manager, and update only the two UIs' Ambari configs. Run on the Ambari host as root, from the cloned repo with a `conf/ca.properties` listing every cluster host:
   `python3 bin/ssl_manager.py --ca --properties=conf/ca.properties --isOverwrite --enable-ssl --ui=NIFI2,NIFI_REGISTRY2 --host <ambari> --cluster <name> --protocol=http --port=8080`
   Without `--scpKeyFile` the tool skips copying the certificates (it needs root ssh to every host, which is disabled here).
2. Copy `keystore.jks` and `truststore.jks` of each host to `/etc/security/certificates` on that host (directory 755, files 644). Ranger Admin's host needs them too: its keystore identity is the client NiFi authorises.
3. Restart NiFi 2 and NiFi Registry 2. They now listen on https (9191 and 62443 here).
4. Run `bin/post_ssl_steps.py --steps ranger-repos,nifi-ranger` from a machine that can resolve the cluster host names. `ranger-repos` moves the repos to https with `SSL` authentication and the client keystore, and `nifi-ranger` creates Ranger users for the certificate identity of Ranger Admin and every node, plus the `/resources` or `/policies` READ and `/proxy` WRITE policies. The order matters: `nifi-ranger` only acts on repos that already use https.
5. Set the audit destinations (section 2) on `ranger-nifi2-audit` and `ranger-nifi-registry2-audit`: HDFS and Solr true, real directory and ZooKeeper quorum. Restart.
6. Solr audit from NiFi needs SolrJ 8.11.3. The NiFi Ranger bundle ships SolrJ 9.7 with Jetty 12 and fails with `NoClassDefFoundError: org/eclipse/jetty/client/util/SPNEGOAuthentication`. Until the fixed NAR is built (NiFi branch `ODP-8280`), replace the SolrJ 9 and Jetty 12 jars in `nifi-ranger-nar-*.nar` (`NAR-INF/bundled-dependencies`) and in `nifi-registry2/ext/ranger/lib` with `solr-solrj-8.11.3.jar` and the Jetty 9.4 http/io/util jars, clear the unpacked NAR work directory and restart. Keep the originals.

Result: Test Connection passes, 2 NiFi plugins and 1 Registry plugin on the latest policy version, audit events in Solr and HDFS.

Note: the SSL manager itself failed on Python 3.12 (`RawConfigParser.readfp` was removed). That is fixed on the `ODP-8311` branch.

## 6. Knox: put the plugin in the request path

The Knox plugin only runs if the topology authorizes through Ranger. Check the topology content in Ambari (`topology` config):

```
<provider><role>authorization</role><name>XASecurePDPKnox</name><enabled>true</enabled></provider>
```

`AclsAuthz` means Ranger is bypassed: the plugin does not register and nothing is audited. The Knox service advisor makes this switch when the plugin is enabled from the UI;
clusters built from a blueprint can keep `AclsAuthz`. Change it and restart Knox.

## 7. Generate traffic to confirm audits

Reads that are allowed are not always audited, so test with both an allowed and a denied request:

| Service | Request |
|---|---|
| HDFS | any `hdfs dfs -ls` as a non-superuser |
| Hive | `beeline -u "jdbc:hive2://<hs2>:10000/;principal=hive/_HOST@<REALM>" -e "show databases"` |
| Knox | `curl -k -u <user> https://<knox>:8443/gateway/default/webhdfs/v1/?op=LISTSTATUS` |
| Atlas | a write the user is not allowed to do (`POST /api/atlas/v2/types/typedefs`); denied calls are audited |
| Kudu | `kudu table create <master> <json>` as a user without a policy |
| Schema Registry | `POST /api/v1/schemaregistry/schemas` (delete the probe schema afterwards) |

Then check Solr and HDFS as in section 1. Audit batches flush on a timer, so allow about a minute.

## 8. Checking everything at once

A small script used during this work lists, per repo: Test Connection result, the newest Solr audit event, the newest HDFS audit file and how many plugins are on the
latest policy version. The data sources are the Ranger public API (`/service/public/v2/api/service`, `/service/plugins/services/validateConfig`,
`/service/assets/accessAudit`, `/service/plugins/plugins/info`) and `hdfs dfs -ls -R /ranger/audit`.

## 9. Known gaps on this cluster

- KMS: HDFS audit works; its Solr events are not shown in the admin audit view.
- HBase: one of five plugins (the `hbaseMaster` on the first host) stays on an older policy version. It is probably a standby master that has not started its plugin; not confirmed.
- The Trino and Knox certificate imports, the Knox demo LDAP start, the Ranger Admin truststore change and the SolrJ replacement in the NiFi NAR are manual on this cluster. They are not in the mpack or blueprints, so a rebuilt cluster needs them again.
