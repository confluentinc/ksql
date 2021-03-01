---
layout: page
title: ksqlDB Server Configuration Parameter Reference
tagline: Refer ksqlDB's server parameters
description: Parameters for onfiguring ksqlDB Server
keywords: ksqldb, configure, server, setup, install
---

# Server configuration

These configuration parameters control the general behavior of ksqlDB server.
Many parameters can only be set once for the entire server, and must be
specified using the `ksql-server.properties` file. Some parameters, however,
can be set on a per-persistent query basis using `SET`. This is indicated in each parameter
section below.

For more information on setting properties, see
[Configure ksqlDB Server](/operate-and-deploy/installation/server-config).

!!! important
	ksqlDB Server configuration settings take precedence over those set in the
    ksqlDB CLI. For example, if a value for `ksql.streams.replication.factor`
    is set in both ksqlDB Server and ksqlDB CLI, the ksqlDB Server value is
    used.

!!! tip
	Each property has a corresponding environment variable in the Docker
    image for
    [ksqlDB Server](https://hub.docker.com/r/confluentinc/ksqldb-server/). The
    environment variable name is constructed from the configuration property
    name by converting to uppercase, replacing periods with underscores, and
    prepending with `KSQL_`. For example, the name of the `ksql.service.id`
    environment variable is `KSQL_KSQL_SERVICE_ID`. For more information,
    see [Install ksqlDB with Docker](/operate-and-deploy/installation/install-ksqldb-with-docker).

## `ksql.advertised.listener`

This is the URL used for inter-node communication.  Unlike `listeners` or `ksql.internal.listener`,
this configuration doesn't create a listener. Instead, it is used to set an externally routable
URL that other ksqlDB nodes will use to communicate with this node. It only needs to be set if
the internal listener is not externally resolvable or routable.

If not set, the default behavior is to use the internal listener, which is controlled by `ksql.internal.listener`.

If `ksql.internal.listener` resolves to a URL that uses `localhost`, a wildcard IP address,
like `0.0.0.0`, or a hostname that other ksqlDB nodes either can't resolve or can't route requests
to, set `ksql.advertised.listener` to a URL that ksqlDB nodes can resolve.

For more information, see [Configuring Listeners of a ksqlDB Cluster](./index.md#configuring-listeners-of-a-ksqldb-cluster)

## `ksql.connect.url`

The {{ site.kconnect }} cluster URL to integrate with. If the
{{ site.kconnect }} cluster is running locally to the ksqlDB Server,
use `localhost` and the configuration port specified in the
{{ site.kconnect }} configuration file.

## `ksql.connect.worker.config`

The connect worker configuration file, if spinning up {{ site.kconnect }}
alongside the ksqlDB server. Don't set this property if you're using
an external `ksql.connect.url`.

## `ksql.fail.on.deserialization.error`

**Per query:** yes

Indicates whether to fail if corrupt messages are read. ksqlDB decodes
messages at runtime when reading from a Kafka topic. The decoding that
ksqlDB uses depends on what's defined in STREAM's or TABLE's data
definition as the data format for the topic. If a message in the topic
can't be decoded according to that data format, ksqlDB considers this
message to be corrupt.

For example, a message is corrupt if ksqlDB expects message values to be in
JSON format, but it's in DELIMITED format instead. The default value in ksqlDB
is `false`, which means a corrupt message results in a log entry, and ksqlDB
continues processing. To change this default behavior and instead have
{{ site.kstreams }} threads shut down when corrupt messages are encountered,
add the following setting to your ksqlDB Server properties file:

## `ksql.fail.on.production.error`

**Per query:** yes

Indicates whether to fail if ksqlDB fails to publish a record to an output
topic due to a {{ site.ak }} producer exception. The default value in ksqlDB is
`true`, which means if a producer error occurs, then the {{ site.kstreams }}
thread that encountered the error will shut down. To log the error
message to the
[Processing Log](/reference/processing-log)
and have ksqlDB continue processing as normal, add the following setting
to your ksqlDB Server properties file:

```properties
ksql.fail.on.production.error=false
```

## `ksql.functions.substring.legacy.args`

**Per query:** yes

Controls the semantics of the SUBSTRING UDF. Refer to the SUBSTRING
documentation in the [function](/developer-guide/ksqldb-reference/scalar-functions)
guide for details.

## `ksql.internal.listener`

The `ksql.internal.listener` setting controls the address bound for use by internal,
intra-cluster communication.

If not set, the internal listener defaults to the first listener defined by `listeners`.

This setting is most often useful in a IaaS environment to separate external-facing
traffic from internal traffic.

## `ksql.internal.topic.replicas`

**Per query:** yes

The number of replicas for the internal topics created by ksqlDB Server.
The default is 1. Replicas for the record processing log topic should be
configured separately. For more information, see
[Processing Log](/reference/processing-log).

## `ksql.logging.processing.topic.auto.create`

Toggles automatic processing log topic creation. If set to true, ksqlDB
automatically tries to create a processing log topic at startup.
The name of the topic is the value of the
[ksql.logging.processing.topic.name](#ksqlloggingprocessingtopicname) property.
The number of partitions is taken from the
[ksql.logging.processing.topic.partitions](#ksqlloggingprocessingtopicpartitions)
property , and the replication factor is taken from the
[ksql.logging.processing.topic.replication.factor](#ksqlloggingprocessingtopicreplicationfactor)
property. By default, this property has the value `false`.

## `ksql.logging.processing.topic.name`

If automatic processing log topic creation is enabled, ksqlDB sets the
name of the topic to the value of this property. If automatic processing
log stream creation is enabled, ksqlDB uses this topic to back the stream.
By default, this property has the value
`<service id>ksql_processing_log`, where `<service id>` is the value of
the [ksql.service.id](#ksqlserviceid) property.

## `ksql.logging.processing.topic.partitions`

If automatic processing log topic creation is enabled, ksqlDB creates the
topic with number of partitions set to the value of this property. By
default, this property has the value `1`.

## `ksql.logging.processing.topic.replication.factor`

If automatic processing log topic creation is enabled, ksqlDB creates the
topic with number of replicas set to the value of this property. By
default, this property has the value `1`.

## `ksql.logging.processing.stream.auto.create`

Toggles automatic processing log stream creation. If set to true, and
ksqlDB is running in interactive mode on a new cluster, ksqlDB automatically
creates a processing log stream when it starts up. The name for the
stream is the value of the 
[ksql.logging.processing.stream.name](#ksqlloggingprocessingstreamname)
property. The stream is created over the topic set in the
[ksql.logging.processing.topic.name](#ksqlloggingprocessingtopicname) property
By default, this property has the value `false`.

## `ksql.logging.processing.stream.name`

If automatic processing log stream creation is enabled, ksqlDB sets the
name of the stream to the value of this property. By default, this
property has the value `KSQL_PROCESSING_LOG`.

## `ksql.logging.processing.rows.include`

Toggles whether or not the processing log should include rows in log
messages. By default, this property has the value `false`.

## `ksql.logging.server.rate.limited.response.codes`

A list of `code:qps` pairs, to limit the rate of server request
logging.  An example would be "400:10" which would limit 400 error
logs to 10 per second.  This is useful for limiting certain 4XX errors that you
might not want to blow up in the logs.
This setting enables seeing the logs when the request rate is low 
and dropping them when they go over the threshold.
A message will be logged every 5 seconds indicating if the rate limit
is being hit, so an absence of this message means a complete set of logs.

## `ksql.logging.server.rate.limited.request.paths`

A list of `path:qps` pairs, to limit the rate of server request
logging.  An example would be "/query:10" which would limit pull query
logs to 10 per second. This is useful for requests that are coming in
at a high rate, such as for pull queries.
This setting enables seeing the logs when the request rate is low
and dropping them when they go over the threshold.
A message will be logged every 5 seconds indicating if the rate limit
is being hit, so an absence of this message means a complete set of logs.

## `ksql.metrics.tags.custom`

A list of tags to be included with emitted
[JMX metrics](/operate-and-deploy/monitoring), formatted as
a string of `key:value` pairs separated by commas. For example,
`key1:value1,key2:value2`.

## `ksql.output.topic.name.prefix`

The default prefix for automatically created topic names. Unless a user
defines an explicit topic name in a SQL statement, ksqlDB prepends the
value of `ksql.output.topic.name.prefix` to the names of automatically
created output topics. For example, you might use "ksql-interactive-"
to name output topics in a ksqlDB Server cluster that's deployed in
interactive mode. For more information, see
[Interactive ksqlDB clusters](/operate-and-deploy/installation/server-config/security#interactive-ksqldb-clusters).

## `ksql.persistence.default.format.key`

**Per query:** yes

Sets the default value for the `KEY_FORMAT` property if one is
not supplied explicitly in [CREATE TABLE](/developer-guide/ksqldb-reference/create-table)
or [CREATE STREAM](/developer-guide/ksqldb-reference/create-stream)
statements.

The default value for this configuration is `KAFKA`.

If not set and no explicit key format is provided in the statement, via either the `KEY_FORMAT` or the
`FORMAT` property, the statement will be rejected as invalid.

For supported formats, see [Serialization Formats](/reference/serialization).

[CREATE STREAM AS SELECT](/developer-guide/ksqldb-reference/create-stream-as-select) and
[CREATE TABLE AS SELECT](/developer-guide/ksqldb-reference/create-table-as-select) 
statements that create streams or tables with key columns, where the source stream or table 
has a [NONE](/reference/serialization#none) key format, will also use the default
key format set in this configuration if no explicit key format is declared in the `WITH` clause.

## `ksql.persistence.default.format.value`

**Per query:** yes

Sets the default value for the `VALUE_FORMAT` property if one is
not supplied explicitly in [CREATE TABLE](/developer-guide/ksqldb-reference/create-table)
or [CREATE STREAM](/developer-guide/ksqldb-reference/create-stream)
statements.

If not set and no explicit value format is provided in the statement, via either the `VALUE_FORMAT` or the
`FORMAT` property, the statement will be rejected as invalid.

For supported formats, see [Serialization Formats](/reference/serialization).

## `ksql.persistence.wrap.single.values`

**Per query:** yes

Sets the default value for the `WRAP_SINGLE_VALUE` property if one is
not supplied explicitly in [CREATE TABLE](/developer-guide/ksqldb-reference/create-table),
[CREATE STREAM](/developer-guide/ksqldb-reference/create-stream),
[CREATE TABLE AS SELECT](/developer-guide/ksqldb-reference/create-table-as-select) or
[CREATE STREAM AS SELECT](/developer-guide/ksqldb-reference/create-stream-as-select)
statements.

If not set and no explicit value is provided in the statement, the value format's default wrapping 
is used.

When set to `true`, ksqlDB serializes the column value nested within a JSON object, Avro record,
or Protobuf message, depending on the format in use. When set to `false`, ksqlDB persists the column
value without any nesting, as an anonymous value.

For example, consider the statement:

```sql
CREATE STREAM y AS SELECT f0 FROM x EMIT CHANGES;
```

The statement selects a single field as the value of stream `y`. If `f0`
has the integer value `10`, with `ksql.persistence.wrap.single.values`
set to `true`, the JSON format persists the value within a JSON object,
as it would if the value had more fields:

```json
{
   "F0": 10
}
```

With `ksql.persistence.wrap.single.values` set to `false`, the JSON
format persists the single field's value as a JSON number: `10`.

```json
10
```

The properties control whether or not the field's value is written as a named field within a
record or as an anonymous value.

This setting can be toggled using the SET command

```sql
SET 'ksql.persistence.wrap.single.values'='false';
```

For more information, refer to the
[CREATE TABLE](/developer-guide/ksqldb-reference/create-table),
[CREATE STREAM](/developer-guide/ksqldb-reference/create-stream),
[CREATE TABLE AS SELECT](/developer-guide/ksqldb-reference/create-table-as-select) or
[CREATE STREAM AS SELECT](/developer-guide/ksqldb-reference/create-stream-as-select)
statements.

!!! note
    Not all formats support wrapping and unwrapping. If you use a format that doesn't support
    the default value you set, the format ignores the setting. For information on which formats
    support wrapping and unwrapping, see the [serialization docs](/reference/serialization).

## `ksql.schema.registry.url`

**Per query:** yes

The {{ site.sr }} URL path to connect ksqlDB to. To communicate with {{ site.sr }}
over a secure connection, see
[Configure ksqlDB for Secured {{ site.srlong }}](/operate-and-deploy/installation/server-config/security#configure-ksqldb-for-https).

## `ksql.service.id`

**Per query:** yes

The service ID of the ksqlDB server. This is used to define the ksqlDB
cluster membership of a ksqlDB Server instance.

- If multiple ksqlDB servers connect to the same {{ site.ak }} cluster
  (i.e. the same `bootstrap.servers` *and* the same `ksql.service.id`)
  they form a ksqlDB cluster and share the workload.

- If multiple ksqlDB servers connect to the same {{ site.ak }} cluster but
  *don't* have the same `ksql.service.id`, they each get a different command
  topic and form separate ksqlDB clusters, by `ksql.service.id`.  

By default, the service ID of ksqlDB servers is `default_`. The service ID
is also used as the prefix for the internal topics created by ksqlDB.
Using the default value `ksql.service.id`, the ksqlDB internal topics will
be prefixed as `_confluent-ksql-default_`. For example, `_command_topic`
becomes `_confluent-ksql-default__command_topic`).

!!! important
    By convention, the `ksql.service.id` property should end with a
    separator character of some form, like a dash or underscore, as
    this makes the internal topic names easier to read.

## `ksql.streams.auto.offset.reset`

Determines what to do when there is no initial offset in {{ site.aktm }}
or if the current offset doesn't exist on the server. The default
value in ksqlDB is `latest`, which means all {{ site.ak }} topics are read from
the latest available offset. For example, to change it to `earliest` by
using the ksqlDB CLI:

```sql
SET 'auto.offset.reset'='earliest';
```

For more information, see [Kafka Consumer](https://docs.confluent.io/current/clients/consumer.html) and
[AUTO_OFFSET_RESET_CONFIG](https://docs.confluent.io/{{ site.release }}/clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html#AUTO_OFFSET_RESET_CONFIG).

## `ksql.streams.bootstrap.servers`

A list of host and port pairs that is used for establishing the initial
connection to the Kafka cluster. This list should be in the form
`host1:port1,host2:port2,...` The default value in ksqlDB is
`localhost:9092`. For example, to change it to `9095` by using the ksqlDB CLI:

```sql
SET 'bootstrap.servers'='localhost:9095';
```

For more information, see
[Streams parameter reference](https://docs.confluent.io/current/streams/developer-guide/config-streams.html#configuration-parameter-reference)
and 
[BOOTSTRAP_SERVERS_CONFIG](https://docs.confluent.io/{{ site.release }}/streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#BOOTSTRAP_SERVERS_CONFIG).

## `ksql.streams.commit.interval.ms`

The frequency to save the position of the processor. The default value
in ksqlDB is `2000`. Here is an example to change the value to `5000` by
using the ksqlDB CLI:

```sql
SET 'commit.interval.ms'='5000';
```

For more information, see the
[Streams parameter reference](https://docs.confluent.io/current/streams/developer-guide/config-streams.html#optional-configuration-parameters)
and 
[COMMIT_INTERVAL_MS_CONFIG](https://docs.confluent.io/{{ site.release }}/streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#COMMIT_INTERVAL_MS_CONFIG),

## `ksql.streams.cache.max.bytes.buffering`

The maximum number of memory bytes to be used for buffering across all
threads. The default value in ksqlDB is `10000000` (~ 10 MB). Here is an
example to change the value to `20000000` by using the ksqlDB CLI:

```sql
SET 'cache.max.bytes.buffering'='20000000';
```

For more information, see the
[Streams parameter reference](https://docs.confluent.io/current/streams/developer-guide/config-streams.html#optional-configuration-parameters)
and
[CACHE_MAX_BYTES_BUFFERING_CONFIG](https://docs.confluent.io/{{ site.release }}/streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#CACHE_MAX_BYTES_BUFFERING_CONFIG).

## `ksql.streams.num.stream.threads`

This number of stream threads in an instance of the {{ site.kstreams }}
application. The stream processing code runs in these threads. For more
information about the {{ site.kstreams }} threading model, see
[Threading Model](https://docs.confluent.io/current/streams/architecture.html#threading-model).

## ksql.streams.processing.guarantee

The processing semantics to use for persistent queries. The default is 
`at_least_once`. To enable exactly-once semantics, use `exactly_once`. 

For more information, see [Processing Guarantees](/operate-and-deploy/exactly-once-semantics).

## `ksql.streams.state.dir`

Sets the storage directory for stateful operations, like aggregations and
joins, to a durable location. By default, state is stored in the
`/tmp/kafka-streams` directory.

!!! note
    The state storage directory must be unique for every server running on the
    machine. Otherwise, servers may appear to be stuck and not doing any work.

## `ksql.queries.file`

A file that specifies a predefined set of queries for the ksqlDB cluster.
For an example, see
[Non-interactive (Headless) ksqlDB Usage](index.md#non-interactive-headless-ksqldb-usage).

## `ksql.query.persistent.active.limit`

The maximum number of persistent queries that may be running at any
given time. Applies to interactive mode only. Once the limit is reached,
commands that try to start additional persistent queries will be
rejected. Users may terminate existing queries before attempting to
start new ones to avoid hitting the limit. The default is no limit.

When setting up ksqlDB servers, it may be desirable to configure this
limit to prevent users from overloading the server with too many
queries, since throughput suffers as more queries are run
simultaneously, and also because there is some small CPU overhead
associated with starting each new query. For more information, see
[Sizing Recommendations](/operate-and-deploy/capacity-planning).

## `ksql.query.pull.enable.standby.reads`

**Per query:** yes

Config to enable/disable forwarding pull queries to standby hosts when the active is dead. This means that stale values may be returned 
for these queries since standby hosts receive updates from the changelog topic (to which the active writes to) asynchronously.
Turning on this configuration, effectively sacrifices consistency for higher availability. 

Setting to `true` guarantees high availability for pull queries. If set to `false`, pull queries will fail when the active is dead and 
until a new active is elected. Default value is `false`. 

For using this functionality, the server must be configured with `ksql.streams.num.standby.replicas` >= `1`, so standbys are actually enabled for the 
underlying Kafka Streams topologies. We also recommend `ksql.heartbeat.enable=true`, to ensure pull queries quickly route around dead/failed servers, 
without wastefully attempting to open connections to it (which can be slow & resource in-efficient). 

## `ksql.query.pull.max.allowed.offset.lag`

**Per query:** yes

Config to control the maximum lag tolerated by a pull query against a table, expressed as the number of messages a given table-partition is behind, compared to the 
changelog topic. This is applied to all servers, both active and standbys included. This can be overridden per query, from the CLI (using `SET` command) or 
the pull query REST endpoint (by including it in the request e.g: `"streamsProperties": {"ksql.query.pull.max.allowed.offset.lag": "100"}`). 

By default, any amount of lag is allowed. For using this functionality, the server must be configured with `ksql.heartbeat.enable=true` and 
`ksql.lag.reporting.enable=true`, so the servers can exchange lag information between themselves ahead of time, to validate pull queries against the allowed lag.

## `ksql.variable.substitution.enable`

Enables variable substitution through [`DEFINE`](../../../../developer-guide/ksqldb-reference/define) statements.

## `listeners`

The `listeners` setting controls the REST API endpoint for the ksqlDB
Server. For more info, see
[ksqlDB REST API Reference](/developer-guide/api).

The default `listeners` is `http://0.0.0.0:8088`, which binds to all
IPv4 interfaces. Set `listeners` to `http://[::]:8088` to bind to all
IPv6 interfaces. Update this to a specific interface to bind only to a
single interface. For example:

```properties
# Bind to all IPv4 interfaces.
listeners=http://0.0.0.0:8088

# Bind to all IPv6 interfaces.
listeners=http://[::]:8088

# Bind only to localhost.
listeners=http://localhost:8088

# Bind to specific hostname or ip.
listeners=http://server1245:8088
```

You can configure ksqlDB Server to use HTTPS. For more information, see
[Configure ksqlDB for HTTPS](/operate-and-deploy/installation/server-config/security#configure-ksqldb-for-https).

## `response.http.headers.config`

Use to select which HTTP headers are returned in the HTTP response for {{ site.cp }}
components. Specify multiple values in a comma-separated string using the
format ``[action][header name]:[header value]`` where ``[action]`` is one of
the following: ``set``, ``add``, ``setDate``, or ``addDate``. You must use
quotation marks around the header value when the header value contains commas,
for example: 

```properties
response.http.headers.config="add Cache-Control: no-cache, no-store, must-revalidate", add X-XSS-Protection: 1; mode=block, add Strict-Transport-Security: max-age=31536000; includeSubDomains, add X-Content-Type-Options: nosniff  
```

## `ksql.sink.partitions` (Deprecated)

**Per query:** yes

The default number of partitions for the topics created by ksqlDB. The
default is four. This property has been deprecated.
For more info see the WITH clause properties in
[CREATE STREAM AS SELECT](/developer-guide/ksqldb-reference/create-stream-as-select) and
[CREATE TABLE AS SELECT](/developer-guide/ksqldb-reference/create-table-as-select).

## `ksql.sink.replicas` (Deprecated)

**Per query:** yes

The default number of replicas for the topics created by ksqlDB. The
default is one. This property has been deprecated. For
more info see the WITH clause properties in
[CREATE STREAM AS SELECT](/developer-guide/ksqldb-reference/create-stream-as-select) and
[CREATE TABLE AS SELECT](/developer-guide/ksqldb-reference/create-table-as-select).