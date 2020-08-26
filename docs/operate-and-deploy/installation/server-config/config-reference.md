---
layout: page
title: ksqlDB Configuration Parameter Reference
tagline: Set up ksqlDB Server
description: Settings for configuring ksqlDB Server
keywords: ksqldb, configure, server, setup, install
---

Here are some common configuration properties that you can customize.
For more information on setting properties, see
[Configure ksqlDB Server](index.md) and
[Configure ksqlDB CLI](../cli-config.md).

!!! tip
	Each property has a corresponding environment variable in the Docker
    image for
    [ksqlDB Server](https://hub.docker.com/r/confluentinc/ksqldb-server/). The
    environment variable name is constructed from the configuration property
    name by converting to uppercase, replacing periods with underscores, and
    prepending with `KSQL_`. For example, the name of the `ksql.service.id`
    environment variable is `KSQL_KSQL_SERVICE_ID`. For more information,
    see [Install ksqlDB with Docker](../install-ksqldb-with-docker.md).

Kafka Streams and Kafka Client Settings
---------------------------------------

These configurations control how {{ site.kstreams }} executes queries. These
configurations can be specified via the `ksql-server.properties` file or
via `SET` in a ksqlDB CLI. These can be provided with the optional
`ksql.streams.` prefix.

!!! important
	Although you can use either prefixed (`ksql.streams.`) or un-prefixed
    settings, we recommend that you use prefixed settings.

### ksql.streams.auto.offset.reset

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

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/)
is `KSQL_KSQL_STREAMS_AUTO_OFFSET_RESET`.

### ksql.streams.bootstrap.servers

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

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/)
is `KSQL_KSQL_STREAMS_BOOTSTRAP_SERVERS` or `KSQL_BOOTSTRAP_SERVERS`. For
more information, see
[Install ksqlDB with Docker](../install-ksqldb-with-docker.md).

### ksql.streams.commit.interval.ms

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

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/)
is `KSQL_KSQL_STREAMS_COMMIT_INTERVAL_MS`.

### ksql.streams.cache.max.bytes.buffering

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

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/)
is `KSQL_KSQL_STREAMS_CACHE_MAX_BYTES_BUFFERING`.

### ksql.streams.num.stream.threads

This number of stream threads in an instance of the {{ site.kstreams }}
application. The stream processing code runs in these threads. For more
information about the {{ site.kstreams }} threading model, see
[Threading Model](https://docs.confluent.io/current/streams/architecture.html#threading-model).

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/)
is `KSQL_KSQL_STREAMS_NUM_STREAM_THREADS`.

### ksql.streams.processing.guarantee

The processing semantics to use for persistent queries. The default is 
`at_least_once`. To enable exactly-once semantics, use `exactly_once`. 

For more information, see [Processing Guarantees](../../../concepts/processing-guarantees.md).

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/)
is `KSQL_KSQL_STREAMS_PROCESSING_GUARANTEE`.

### ksql.output.topic.name.prefix

The default prefix for automatically created topic names. Unless a user
defines an explicit topic name in a SQL statement, ksqlDB prepends the
value of `ksql.output.topic.name.prefix` to the names of automatically
created output topics. For example, you might use "ksql-interactive-"
to name output topics in a ksqlDB Server cluster that's deployed in
interactive mode. For more information, see
[Interactive ksqlDB clusters](security.md#interactive-ksqldb-clusters).

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/)
is `KSQL_KSQL_OUTPUT_TOPIC_NAME_PREFIX`.

ksqlDB Query Settings
---------------------

These configurations control how ksqlDB executes queries. These
configurations can be specified via the `ksql-server.properties` file or
via `SET` in ksqlDB CLI, for example, `ksql.service.id` and
`ksql.persistent.prefix`.

### ksql.fail.on.deserialization.error

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

```properties
ksql.fail.on.deserialization.error=true
```

### ksql.fail.on.production.error

Indicates whether to fail if ksqlDB fails to publish a record to an output
topic due to a {{ site.ak }} producer exception. The default value in ksqlDB is
`true`, which means if a producer error occurs, then the {{ site.kstreams }}
thread that encountered the error will shut down. To log the error
message to the
[Processing Log](../../../developer-guide/test-and-debug/processing-log.md)
and have ksqlDB continue processing as normal, add the following setting
to your ksqlDB Server properties file:

```properties
ksql.fail.on.production.error=false
```

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/)
is `KSQL_KSQL_FAIL_ON_DESERIALIZATION_ERROR`.

### ksql.schema.registry.url

The {{ site.sr }} URL path to connect ksqlDB to. To communicate with {{ site.sr }}
over a secure connection, see
[Configure ksqlDB for Secured {{ site.srlong }}](security.md#configure-ksqldb-for-https).

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/)
is `KSQL_KSQL_SCHEMA_REGISTRY_URL`.

### ksql.service.id

The service ID of the ksqlDB server. This is used to define the ksqlDB
cluster membership of a ksqlDB Server instance. If multiple ksqlDB servers
connect to the same {{ site.ak }} cluster (i.e. the same `bootstrap.servers`
*and* the same `ksql.service.id`) they will form a ksqlDB cluster and
share the workload.

By default, the service ID of ksqlDB servers is `default_`. The service ID
is also used as the prefix for the internal topics created by ksqlDB.
Using the default value `ksql.service.id`, the ksqlDB internal topics will
be prefixed as `_confluent-ksql-default_`. For example, `_command_topic`
becomes `_confluent-ksql-default__command_topic`).

!!! important
    By convention, the `ksql.service.id` property should end with a
    separator character of some form, like a dash or underscore, as
    this makes the internal topic names easier to read.

### ksql.internal.topic.replicas

The number of replicas for the internal topics created by ksqlDB Server.
The default is 1. Replicas for the record processing log topic should be
configured separately. For more information, see
[Processing Log](../../../developer-guide/test-and-debug/processing-log.md).

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/)
is `KSQL_KSQL_INTERNAL_TOPIC_REPLICAS`.

### ksql.sink.partitions (Deprecated)

The default number of partitions for the topics created by ksqlDB. The
default is four. This property has been deprecated.
For more info see the WITH clause properties in
[CREATE STREAM AS SELECT](../../../developer-guide/ksqldb-reference/create-stream-as-select.md) and
[CREATE TABLE AS SELECT](../../../developer-guide/ksqldb-reference/create-table-as-select.md).

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/) is
`KSQL_KSQL_SINK_PARTITIONS`.

### ksql.sink.replicas (Deprecated)

The default number of replicas for the topics created by ksqlDB. The
default is one. This property has been deprecated. For
more info see the WITH clause properties in
[CREATE STREAM AS SELECT](../../../developer-guide/ksqldb-reference/create-stream-as-select.md) and
[CREATE TABLE AS SELECT](../../../developer-guide/ksqldb-reference/create-table-as-select.md).

### ksql.functions.substring.legacy.args

Controls the semantics of the SUBSTRING UDF. Refer to the SUBSTRING
documentation in the [function](../../../developer-guide/ksqldb-reference/scalar-functions.md)
guide for details.

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/)
is `KSQL_KSQL_FUNCTIONS_SUBSTRING_LEGACY_ARGS`.

### ksql.persistence.wrap.single.values

Sets the default value for the `WRAP_SINGLE_VALUE` property if one is
not supplied explicitly in [CREATE TABLE](../../../developer-guide/ksqldb-reference/create-table.md),
[CREATE STREAM](../../../developer-guide/ksqldb-reference/create-stream.md),
[CREATE TABLE AS SELECT](../../../developer-guide/ksqldb-reference/create-table-as-select.md) or
[CREATE STREAM AS SELECT](../../../developer-guide/ksqldb-reference/create-stream-as-select.md)
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
[CREATE TABLE](../../../developer-guide/ksqldb-reference/create-table.md),
[CREATE STREAM](../../../developer-guide/ksqldb-reference/create-stream.md),
[CREATE TABLE AS SELECT](../../../developer-guide/ksqldb-reference/create-table-as-select.md) or
[CREATE STREAM AS SELECT](../../../developer-guide/ksqldb-reference/create-stream-as-select.md)
statements.

 !!! note
   Not all formats support wrapping and unwrapping. If you use a format that doesn't support
   the default value you set, the format ignores the setting. For information on which formats
   support wrapping and unwrapping, see the [serialization docs](../../../developer-guide/serialization.md).

### ksql.query.pull.enable.standby.reads

Config to enable/disable forwarding pull queries to standby hosts when the active is dead. This means that stale values may be returned 
for these queries since standby hosts receive updates from the changelog topic (to which the active writes to) asynchronously.
Turning on this configuration, effectively sacrifices consistency for higher availability. 

Setting to `true` guarantees high availability for pull queries. If set to `false`, pull queries will fail when the active is dead and 
until a new active is elected. Default value is `false`. 

For using this functionality, the server must be configured with `ksql.streams.num.standby.replicas` >= `1`, so standbys are actually enabled for the 
underlying Kafka Streams topologies. We also recommend `ksql.heartbeat.enable=true`, to ensure pull queries quickly route around dead/failed servers, 
without wastefully attempting to open connections to it (which can be slow & resource in-efficient). 

### ksql.query.pull.max.allowed.offset.lag

Config to control the maximum lag tolerated by a pull query against a table, expressed as the number of messages a given table-partition is behind, compared to the 
changelog topic. This is applied to all servers, both active and standbys included. This can be overridden per query, from the CLI (using `SET` command) or 
the pull query REST endpoint (by including it in the request e.g: `"streamsProperties": {"ksql.query.pull.max.allowed.offset.lag": "100"}`). 

By default, any amount of lag is allowed. For using this functionality, the server must be configured with `ksql.heartbeat.enable=true` and 
`ksql.lag.reporting.enable=true`, so the servers can exchange lag information between themselves ahead of time, to validate pull queries against the allowed lag. 

### ksql.suppress.buffer.size.bytes

Bound the number of bytes that the buffer can use for suppression. Negative size means the buffer 
will be unbounded. If the maximum capacity is exceeded, the query will be terminated.

ksqlDB Server Settings
----------------------

These configurations control the general behavior of ksqlDB Server.
These configurations can only be specified via the
`ksql-server.properties` file.

!!! important
	ksqlDB Server configuration settings take precedence over those set in the
    ksqlDB CLI. For example, if a value for `ksql.streams.replication.factor`
    is set in both ksqlDB Server and ksqlDB CLI, the ksqlDB Server value is
    used.

### ksql.query.persistent.active.limit

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
[Sizing Recommendations](../../capacity-planning.md#recommendations-and-best-practices).

### ksql.queries.file

A file that specifies a predefined set of queries for the ksqlDB cluster.
For an example, see
[Non-interactive (Headless) ksqlDB Usage](index.md#non-interactive-headless-ksqldb-usage).

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/)
is `KSQL_KSQL_QUERIES_FILE`.

### listeners

The `listeners` setting controls the REST API endpoint for the ksqlDB
Server. For more info, see
[ksqlDB REST API Reference](../../../developer-guide/api.md).

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
[Configure ksqlDB for HTTPS](security.md#configure-ksqldb-for-https).

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/)
is `KSQL_LISTENERS`.

### ksql.internal.listener

The `ksql.internal.listener` setting controls the address bound for use by internal,
intra-cluster communication.

If not set, the internal listener defaults to the first listener defined by `listeners`.

This setting is most often useful in a IaaS environment to separate external-facing
traffic from internal traffic.

### ksql.advertised.listener

This is the URL used for inter-node communication.  Unlike `listeners` or `ksql.internal.listener`,
this configuration doesn't create a listener. Instead, it is used to set an externally routable
URL that other ksqlDB nodes will use to communicate with this node. It only needs to be set if
the internal listener is not externally resolvable or routable.

If not set, the default behavior is to use the internal listener, which is controlled by `ksql.internal.listener`.

If `ksql.internal.listener` resolves to a URL that uses `localhost`, a wildcard IP address,
like `0.0.0.0`, or a hostname that other ksqlDB nodes either can't resolve or can't route requests
to, set `ksql.advertised.listeners` to a URL that ksqlDB nodes can resolve.

For more information, see [Configuring Listeners of a ksqlDB Cluster](./index.md#configuring-listeners-of-a-ksqldb-cluster)

### ksql.metrics.tags.custom

A list of tags to be included with emitted
[JMX metrics](../../index.md#monitoring-and-metrics), formatted as
a string of `key:value` pairs separated by commas. For example,
`key1:value1,key2:value2`.

### ksql.streams.state.dir

Sets the storage directory for stateful operations, like aggregations and
joins, to a durable location. By default, state is stored in the
`/tmp/kafka-streams` directory.

!!! note
    The state storage directory must be unique for every server running on the
    machine. Otherwise, servers may appear to be stuck and not doing any work.

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/)
is `KSQL_KSQL_STREAMS_STATE_DIR`.

Confluent Control Center Settings
---------------------------------

You can access ksqlDB Server by using {{ site.c3 }}. For more information,
see [Control Center Configuration Reference](https://docs.confluent.io/current/control-center/installation/configuration.html#ksql-settings).


Confluent Cloud Settings
------------------------

You can connect ksqlDB Server to {{ site.ccloud }}. For more information,
see [Connecting ksqlDB to Confluent Cloud](https://docs.confluent.io/current/cloud/cp-component/ksql-cloud-config.html).

ksqlDB Server Log Settings
--------------------------

To get DEBUG or INFO output from ksqlDB Server, configure a {{ site.ak }}
appender for the server logs. Assign the following configuration settings in
the ksqlDB Server config file.

```properties
log4j.appender.kafka_appender=org.apache.kafka.log4jappender.KafkaLog4jAppender
log4j.appender.kafka_appender.layout=io.confluent.common.logging.log4j.StructuredJsonLayout
log4j.appender.kafka_appender.BrokerList=localhost:9092
log4j.appender.kafka_appender.Topic=KSQL_LOG
log4j.logger.io.confluent.ksql=INFO,kafka_appender
```

ksqlDB Processing Log Settings
------------------------------

The following configuration settings control the behavior of the
[ksqlDB Processing Log](../../../developer-guide/test-and-debug/processing-log.md).

!!! note
    To enable security for the KSQL Processing Log, assign log4j properties
    as shown in
    [log4j-secure.properties](https://github.com/confluentinc/cp-demo/blob/master/scripts/security/log4j-secure.properties).

### ksql.logging.processing.topic.auto.create

Toggles automatic processing log topic creation. If set to true, ksqlDB
automatically tries to create a processing log topic at startup.
The name of the topic is the value of the
[ksql.logging.processing.topic.name](#ksqlloggingprocessingtopicname) property.
The number of partitions is taken from the
[ksql.logging.processing.topic.partitions](#ksqlloggingprocessingtopicpartitions)
property , and the replication factor is taken from the
[ksql.logging.processing.topic.replication.factor](#ksqlloggingprocessingtopicreplicationfactor)
property. By default, this property has the value `false`.

### ksql.logging.processing.topic.name

If automatic processing log topic creation is enabled, ksqlDB sets the
name of the topic to the value of this property. If automatic processing
log stream creation is enabled, ksqlDB uses this topic to back the stream.
By default, this property has the value
`<service id>ksql_processing_log`, where `<service id>` is the value of
the [ksql.service.id](#ksqlserviceid) property.

### ksql.logging.processing.topic.partitions

If automatic processing log topic creation is enabled, ksqlDB creates the
topic with number of partitions set to the value of this property. By
default, this property has the value `1`.

### ksql.logging.processing.topic.replication.factor

If automatic processing log topic creation is enabled, ksqlDB creates the
topic with number of replicas set to the value of this property. By
default, this property has the value `1`.

### ksql.logging.processing.stream.auto.create

Toggles automatic processing log stream creation. If set to true, and
ksqlDB is running in interactive mode on a new cluster, ksqlDB automatically
creates a processing log stream when it starts up. The name for the
stream is the value of the 
[ksql.logging.processing.stream.name](#ksqlloggingprocessingstreamname)
property. The stream is created over the topic set in the
[ksql.logging.processing.topic.name](#ksqlloggingprocessingtopicname) property
By default, this property has the value `false`.

### ksql.logging.processing.stream.name

If automatic processing log stream creation is enabled, ksqlDB sets the
name of the stream to the value of this property. By default, this
property has the value `KSQL_PROCESSING_LOG`.

### ksql.logging.processing.rows.include

Toggles whether or not the processing log should include rows in log
messages. By default, this property has the value `false`.

ksqlDB-Connect Settings
-----------------------

### ksql.connect.url

The {{ site.kconnect }} cluster URL to integrate with. If the
{{ site.kconnect }} cluster is running locally to the ksqlDB Server,
use `localhost` and the configuration port specified in the
{{ site.kconnect }} configuration file.

### ksql.connect.worker.config

The connect worker configuration file, if spinning up {{ site.kconnect }}
alongside the ksqlDB server. Don't set this property if you're using
an external `ksql.connect.url`.

### ksql.connect.polling.enable

Toggles whether or not to poll connect for new connectors and
automatically register them in ksqlDB.

Recommended ksqlDB Production Settings
--------------------------------------

When deploying ksqlDB to production, the following settings are
recommended in your `/etc/ksqldb/ksql-server.properties` file:

```properties
# Set the batch expiry to Integer.MAX_VALUE to ensure that queries will not
# terminate if the underlying Kafka cluster is unavailable for a period of
# time.
ksql.streams.producer.delivery.timeout.ms=2147483647

# Set the maximum allowable time for the producer to block to
# Long.MAX_VALUE. This allows ksqlDB to pause processing if the underlying
# Kafka cluster is unavailable.
ksql.streams.producer.max.block.ms=9223372036854775807

# Configure underlying Kafka Streams internal topics to achieve better
# fault tolerance and durability, even in the face of Kafka broker failures.
# Highly recommended for mission critical applications.
# Note that a value of 3 requires at least 3 brokers in your Kafka cluster.
ksql.streams.replication.factor=3
ksql.streams.producer.acks=all
ksql.streams.topic.min.insync.replicas=2

# Set the storage directory for stateful operations like aggregations and
# joins to be at a durable location. By default, they are stored in /tmp.
# Note that the following path must be replaced with the actual path.
ksql.streams.state.dir=</some/non-temporary-storage-path/>

# Bump the number of replicas for state storage for stateful operations
# like aggregations and joins. By having two replicas (one main and one
# standby) recovery from node failures is quicker since the state doesn't 
# need to be rebuilt from scratch. This configuration is also essential for
# pull queries to be highly available during node failures.
ksql.streams.num.standby.replicas=1
```

For more information, see the
[ksql-production-server.properties](https://github.com/confluentinc/ksql/blob/master/config/ksql-production-server.properties)
example file.
