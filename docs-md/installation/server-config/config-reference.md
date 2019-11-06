---
layout: page
title: KSQL Configuration Parameter Reference
tagline: Set up KSQL Server
description: Settings for configuring KSQL Server
keywords: ksql, confguration, setup, install
---

KSQL Configuration Parameter Reference
======================================

Here are some common configuration properties that you can customize.
Refer to [Configuring KSQL Server](index.md) and
[Configure KSQL CLI](../cli-config.md#configure-ksql-cli) for details of how to set properties.

!!! tip
	 Each property has a corresponding environment variable in the Docker
    image for [KSQL Server](https://hub.docker.com/r/confluentinc/cp-ksql-server/). The
    environment variable name is constructed from the configuration property
    name by converting to uppercase, replacing periods with underscores, and
    prepending with `KSQL_`. For example, the name of the `ksql.service.id`
    environment variable is `KSQL_KSQL_SERVICE_ID`. For more information,
    see [Install KSQL with Docker](../install-ksql-with-docker.md).

Kafka Streams and Kafka Client Settings
---------------------------------------

These configurations control how Kafka Streams executes queries. These
configurations can be specified via the `ksql-server.properties` file or
via `SET` in a KSQL CLI. These can be provided with the optional
`ksql.streams.` prefix.

!!! important
	 Although you can use either prefixed (`ksql.streams.`) or un-prefixed
    settings, it is recommended that you use prefixed settings.

### ksql.streams.auto.offset.reset

Determines what to do when there is no initial offset in {{ site.aktm }}
or if the current offset doesn't exist on the server. The default
value in KSQL is `latest`, which means all Kafka topics are read from
the latest available offset. For example, to change it to `earliest` by
using the KSQL command line:

```sql
SET 'auto.offset.reset'='earliest';
```

For more information, see [Kafka
Consumer](https://docs.confluent.io/current/clients/consumer.html) and
[AUTO_OFFSET_RESET_CONFIG](https://docs.confluent.io/{{ site.release }}/clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html#AUTO_OFFSET_RESET_CONFIG).

The corresponding environment variable in the [KSQL Server
image](https://hub.docker.com/r/confluentinc/cp-ksql-server/) is
`KSQL_KSQL_STREAMS_AUTO_OFFSET_RESET`.

### ksql.streams.bootstrap.servers

A list of host and port pairs that is used for establishing the initial
connection to the Kafka cluster. This list should be in the form
`host1:port1,host2:port2,...` The default value in KSQL is
`localhost:9092`. For example, to change it to `9095` by using the KSQL
command line:

```sql
SET 'bootstrap.servers'='localhost:9095';
```

For more information, see [Streams parameter
reference](https://docs.confluent.io/current/streams/developer-guide/config-streams.html#configuration-parameter-reference)
and 
[BOOTSTRAP_SERVERS_CONFIG](https://docs.confluent.io/{{ site.release }}/streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#BOOTSTRAP_SERVERS_CONFIG).

The corresponding environment variable in the [KSQL Server
image](https://hub.docker.com/r/confluentinc/cp-ksql-server/) is
`KSQL_KSQL_STREAMS_BOOTSTRAP_SERVERS` or `KSQL_BOOTSTRAP_SERVERS`. For
more information, see [Install KSQL with Docker](../install-ksql-with-docker.md).

### ksql.streams.commit.interval.ms

The frequency to save the position of the processor. The default value
in KSQL is `2000`. Here is an example to change the value to `5000` by
using the KSQL command line:

```sql
SET 'commit.interval.ms'='5000';
```

For more information, see the [Streams parameter
reference](https://docs.confluent.io/current/streams/developer-guide/config-streams.html#optional-configuration-parameters)
and 
[COMMIT_INTERVAL_MS_CONFIG](https://docs.confluent.io/{{ site.release }}/streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#COMMIT_INTERVAL_MS_CONFIG),

The corresponding environment variable in the [KSQL Server
image](https://hub.docker.com/r/confluentinc/cp-ksql-server/) is
`KSQL_KSQL_STREAMS_COMMIT_INTERVAL_MS`.

### ksql.streams.cache.max.bytes.buffering

The maximum number of memory bytes to be used for buffering across all
threads. The default value in KSQL is `10000000` (~ 10 MB). Here is an
example to change the value to `20000000` by using the KSQL command
line:

```sql
SET 'cache.max.bytes.buffering'='20000000';
```

For more information, see the [Streams parameter
reference](https://docs.confluent.io/current/streams/developer-guide/config-streams.html#optional-configuration-parameters)
and
[CACHE_MAX_BYTES_BUFFERING_CONFIG](https://docs.confluent.io/{{ site.release }}/streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#CACHE_MAX_BYTES_BUFFERING_CONFIG).

The corresponding environment variable in the [KSQL Server
image](https://hub.docker.com/r/confluentinc/cp-ksql-server/) is
`KSQL_KSQL_STREAMS_CACHE_MAX_BYTES_BUFFERING`.

### ksql.streams.num.stream.threads

This number of stream threads in an instance of the Kafka Streams
application. The stream processing code runs in these threads. For more
information about Kafka Streams threading model, see [Threading
Model](https://docs.confluent.io/current/streams/architecture.html#threading-model).

The corresponding environment variable in the [KSQL Server
image](https://hub.docker.com/r/confluentinc/cp-ksql-server/) is
`KSQL_KSQL_STREAMS_NUM_STREAM_THREADS`.

### ksql.output.topic.name.prefix

The default prefix for automatically created topic names. Unless a user
defines an explicit topic name in a KSQL statement, KSQL prepends the
value of `ksql.output.topic.name.prefix` to the names of automatically
created output topics. For example, you might use "ksql-interactive-"
to name output topics in a KSQL Server cluster that's deployed in
interactive mode. For more information, see
[Interactive KSQL clusters](security.md#interactive-ksql-clusters).

The corresponding environment variable in the [KSQL Server
image](https://hub.docker.com/r/confluentinc/cp-ksql-server/) is
`KSQL_KSQL_OUTPUT_TOPIC_NAME_PREFIX`.

KSQL Query Settings
-------------------

These configurations control how KSQL executes queries. These
configurations can be specified via the `ksql-server.properties` file or
via `SET` in a KSQL CLI. For example, `ksql.service.id` and
`ksql.persistent.prefix`.

### ksql.fail.on.deserialization.error

Indicates whether to fail if corrupt messages are read. KSQL decodes
messages at runtime when reading from a Kafka topic. The decoding that
KSQL uses depends on what's defined in STREAM's or TABLE's data
definition as the data format for the topic. If a message in the topic
can't be decoded according to that data format, KSQL considers this
message to be corrupt. For example, a message is corrupt if KSQL expects
message values to be in JSON format, but they are in DELIMITED format.
The default value in KSQL is `false`, which means a corrupt message will
result in a log entry, and KSQL will continue processing. To change this
default behavior and instead have Kafka Streams threads shut down when
corrupt messages are encountered, add this to your properties file:

```
ksql.fail.on.deserialization.error=true
```

### ksql.fail.on.production.error

Indicates whether to fail if KSQL fails to publish a record to an output
topic due to a Kafka producer exception. The default value in KSQL is
`true`, which means if a producer error occurs, then the Kafka Streams
thread that encountered the error will shut down. To log the error
message to the [KSQL Processing Log](../../developer-guide/processing-log.md)
and have KSQL continue processing as normal, add this to your properties file:

```
ksql.fail.on.production.error=false
```

The corresponding environment variable in the [KSQL Server
image](https://hub.docker.com/r/confluentinc/cp-ksql-server/) is
`KSQL_KSQL_FAIL_ON_DESERIALIZATION_ERROR`.

### ksql.schema.registry.url

The {{ site.sr }} URL path to connect KSQL to. To communicate with {{ site.sr }}
over a secure connection, see [Configure KSQL for Secured {{ site.srlong }}](security.md#configure-ksql-for-https).

The corresponding environment variable in the [KSQL Server
image](https://hub.docker.com/r/confluentinc/cp-ksql-server/) is
`KSQL_KSQL_SCHEMA_REGISTRY_URL`.

### ksql.service.id

The service ID of the KSQL server. This is used to define the KSQL
cluster membership of a KSQL server instance. If multiple KSQL servers
connect to the same Kafka cluster (i.e. the same `bootstrap.servers`)
*and* have the same `ksql.service.id` they will form a KSQL cluster and
share the workload.

By default, the service ID of KSQL servers is `default_`. The service ID
is also used as the prefix for the internal topics created by KSQL.
Using the default value `ksql.service.id`, the KSQL internal topics will
be prefixed as `_confluent-ksql-default_`. For example, `_command_topic`
becomes `_confluent-ksql-default__command_topic`).

!!! important
    By convention, the `ksql.service.id` property should end with a
    separator character of some form, like a dash or underscore, as
    this makes the internal topic names easier to read.

### ksql.internal.topic.replicas

The number of replicas for the internal topics created by KSQL Server.
The default is 1. This configuration parameter works in KSQL 5.3 and
later. Replicas for the record processing log topic should be configured
separately. For more information, see
[KSQL Processing Log](../../developer-guide/processing-log.md).

The corresponding environment variable in the [KSQL Server
image](https://hub.docker.com/r/confluentinc/cp-ksql-server/) is
`KSQL_KSQL_INTERNAL_TOPIC_REPLICAS`.

### ksql.sink.partitions (Deprecated)

The default number of partitions for the topics created by KSQL. The
default is four. This property has been deprecated since 5.3 release.
For more info see the WITH clause properties in
[CREATE STREAM AS SELECT](../../developer-guide/ksqldb-reference/create-stream-as-select.md) and
[CREATE TABLE AS SELECT](../../developer-guide/ksqldb-reference/create-table-as-select.md).

The corresponding environment variable in the [KSQL Server
image](https://hub.docker.com/r/confluentinc/cp-ksql-server/) is
`KSQL_KSQL_SINK_PARTITIONS`.

### ksql.sink.replicas (Deprecated)

The default number of replicas for the topics created by KSQL. The
default is one. This property has been deprecated since 5.3 release. For
more info see the WITH clause properties in
[CREATE STREAM AS SELECT](../../developer-guide/ksqldb-reference/create-stream-as-select.md) and
[CREATE TABLE AS SELECT](../../developer-guide/ksqldb-reference/create-table-as-select.md).

### ksql.functions.substring.legacy.args

Controls the semantics of the SUBSTRING UDF. Refer to the SUBSTRING
documentation in the [function](../../developer-guide/ksqldb-reference/scalar-functions.md)
guide for details.

When upgrading headless mode KSQL applications from versions 5.0.x or
earlier without updating your queries that use SUBSTRING to match the
new 5.1 behavior, you must set this config to `true` to enforce the
previous SUBSTRING behavior. If possible, however, we recommend that you
update your queries accordingly instead of enabling this configuration
setting.

The corresponding environment variable in the [KSQL Server
image](https://hub.docker.com/r/confluentinc/cp-ksql-server/) is
`KSQL_KSQL_FUNCTIONS_SUBSTRING_LEGACY_ARGS`.

### ksql.persistence.wrap.single.values

Sets the default value for the `WRAP_SINGLE_VALUE` property if one is
not supplied explicitly in [CREATE TABLE](../../developer-guide/ksqldb-reference/create-table.md),
[CREATE STREAM](../../developer-guide/ksqldb-reference/create-stream.md),
[CREATE TABLE AS SELECT](../../developer-guide/ksqldb-reference/create-table-as-select.md) or
[CREATE STREAM AS SELECT](../../developer-guide/ksqldb-reference/create-stream-as-select.md)
statements.

When set to the default value, `true`, KSQL serializes the column value
nested with a JSON object or an Avro record, depending on the format in
use. When set to `false`, KSQL persists the column value without any
nesting.

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

The `AVRO` format supports the same properties. The properties control
whether or not the field's value is written as a named field within an
Avro record or as an anonymous value.

This setting can be toggled using the SET command

```sql
SET 'ksql.persistence.wrap.single.values'='false';
```

For more information, refer to the
[CREATE TABLE](../../developer-guide/ksqldb-reference/create-table.md),
[CREATE STREAM](../../developer-guide/ksqldb-reference/create-stream.md),
[CREATE TABLE AS SELECT](../../developer-guide/ksqldb-reference/create-table-as-select.md) or
[CREATE STREAM AS SELECT](../../developer-guide/ksqldb-reference/create-stream-as-select.md)
statements.

!!! note
	 The `DELIMITED` format is not affected by the
    `ksql.persistence.ensure.value.is.struct` setting, because it has no
    concept of an outer record or structure.

KSQL Server Settings
--------------------

These configurations control the general behavior of the KSQL server.
These configurations can only be specified via the
`ksql-server.properties` file.

!!! important
	 KSQL server configuration settings take precedence over those set in the
    KSQL CLI. For example, if a value for `ksql.streams.replication.factor`
    is set in both the KSQL server and KSQL CLI, the KSQL server value is
    used.

### ksql.query.persistent.active.limit

The maximum number of persistent queries that may be running at any
given time. Applies to interactive mode only. Once the limit is reached,
commands that try to start additional persistent queries will be
rejected. Users may terminate existing queries before attempting to
start new ones to avoid hitting the limit. The default is no limit.

When setting up KSQL servers, it may be desirable to configure this
limit to prevent users from overloading the server with too many
queries, since throughput suffers as more queries are run
simultaneously, and also because there is some small CPU overhead
associated with starting each new query. See
[KSQL Sizing Recommendations](../../capacity-planning.md#recommendations-and-best-practices) for
more details.

### ksql.queries.file

A file that specifies a predefined set of queries for the KSQL and KSQL
server. For an example, see
[Non-interactive (Headless) KSQL Usage](index.md#non-interactive-headless-ksql-usage).

The corresponding environment variable in the [KSQL Server
image](https://hub.docker.com/r/confluentinc/cp-ksql-server/) is
`KSQL_KSQL_QUERIES_FILE`.

### listeners

The `listeners` setting controls the REST API endpoint for the KSQL
server. For more info, see [KSQL REST API Reference](../../developer-guide/api.md).

The default `listeners` is `http://0.0.0.0:8088`, which binds to all
IPv4 interfaces. Set `listeners` to `http://[::]:8088` to bind to all
IPv6 interfaces. Update this to a specific interface to bind only to a
single interface. For example:

```
# Bind to all IPv4 interfaces.
listeners=http://0.0.0.0:8088

# Bind to all IPv6 interfaces.
listeners=http://[::]:8088

# Bind only to localhost.
listeners=http://localhost:8088
```

You can configure KSQL Server to use HTTPS. For more information, see
[Configure KSQL for HTTPS](security.md#configure-ksql-for-https).

The corresponding environment variable in the [KSQL Server
image](https://hub.docker.com/r/confluentinc/cp-ksql-server/) is
`KSQL_LISTENERS`.

### ksql.metrics.tags.custom

A list of tags to be included with emitted
[JMX metrics](../../operations.md#monitoring-and-metrics), formatted as
a string of `key:value` pairs separated by commas. For example,
`key1:value1,key2:value2`.

Confluent Control Center Settings
---------------------------------

You can access KSQL Server by using {{ site.c3 }}. For more information,
see [Control Center Configuration
Reference](https://docs.confluent.io/current/control-center/installation/configuration.html#ksql-settings).


Confluent Cloud Settings
------------------------

You can connect KSQL Server to {{ site.ccloud }}. For more information,
see [Connecting KSQL to Confluent
Cloud](https://docs.confluent.io/current/cloud/connect/ksql-cloud-config.html).

KSQL Server Log Settings
------------------------

To get DEBUG or INFO output from KSQL Server, configure a Kafka appender
for the server logs. Assign the following configuration settings in the
KSQL Server config file.

```
log4j.appender.kafka_appender=org.apache.kafka.log4jappender.KafkaLog4jAppender
log4j.appender.kafka_appender.layout=io.confluent.common.logging.log4j.StructuredJsonLayout
log4j.appender.kafka_appender.BrokerList=localhost:9092
log4j.appender.kafka_appender.Topic=KSQL_LOG
log4j.logger.io.confluent.ksql=INFO,kafka_appender
```

KSQL Processing Log Settings
----------------------------

The following configuration settings control the behavior of the
[KSQL Processing Log](../../developer-guide/processing-log.md).

### ksql.logging.processing.topic.auto.create

Toggles automatic processing log topic creation. If set to true, then
KSQL will automatically try to create a processing log topic at startup.
The name of the topic is the value of the
[ksql.logging.processing.topic.name](#ksqlloggingprocessingtopicname) property.
The number of partitions is taken from the
[ksql.logging.processing.topic.partitions](#ksqlloggingprocessingtopicpartitions)
property , and the replication factor is taken from the
[ksql.logging.processing.topic.replication.factor](#ksqlloggingprocessingtopicreplicationfactor)
property. By default, this property has the value `false`.

### ksql.logging.processing.topic.name

If automatic processing log topic creation is enabled, KSQL sets the
name of the topic to the value of this property. If automatic processing
log stream creation is enabled, KSQL uses this topic to back the stream.
By default, this property has the value
`<service id>ksql_processing_log`, where `<service id>` is the value of
the [ksql.service.id](#ksqlserviceid) property.

### ksql.logging.processing.topic.partitions

If automatic processing log topic creation is enabled, KSQL creates the
topic with number of partitions set to the value of this property. By
default, this property has the value `1`.

### ksql.logging.processing.topic.replication.factor

If automatic processing log topic creation is enabled, KSQL creates the
topic with number of replicas set to the value of this property. By
default, this property has the value `1`.

### ksql.logging.processing.stream.auto.create

Toggles automatic processing log stream creation. If set to true, and
KSQL is running in interactive mode on a new cluster, KSQL automatically
creates a processing log stream when it starts up. The name for the
stream is the value of the  [ksql.logging.processing.stream.name](#ksqlloggingprocessingstreamname)
property. The stream is created over the topic set in the
[ksql.logging.processing.topic.name](#ksqlloggingprocessingtopicname) property
By default, this property has the value `false`.

### ksql.logging.processing.stream.name

If automatic processing log stream creation is enabled, KSQL sets the
name of the stream to the value of this property. By default, this
property has the value `KSQL_PROCESSING_LOG`.

### ksql.logging.processing.rows.include

Toggles whether or not the processing log should include rows in log
messages. By default, this property has the value `false`.

KSQL-Connect Settings
---------------------

### ksql.connect.url

The {{ site.kconnect }} cluster URL to integrate with. If the connect
cluster is running locally to the KSQL server, use localhost and the
configuration port specified in the connect configuration file.

### ksql.connect.worker.config

The connect worker configuration file, if spinning up {{ site.kconnect }}
alongside the KSQL server. Don't set this property if you're using
an external `ksql.connect.url`.

### ksql.connect.polling.enable

Toggles whether or not to poll connect for new connectors and
automatically register them in KSQL.

### ksql.connect.configs.topic

The {{ site.kconnect }} configuration topic. This setting corresponds to
`config.storage.topic` in the {{ site.kconnect }} worker configuration.

Recommended KSQL Production Settings
------------------------------------

When deploying KSQL to production, the following settings are
recommended in your `/etc/ksql/ksql-server.properties` file:

```
# Set the batch expiry to Integer.MAX_VALUE to ensure that queries will not
# terminate if the underlying Kafka cluster is unavailable for a period of
# time.
ksql.streams.producer.delivery.timeout.ms=2147483647

# Set the maximum allowable time for the producer to block to
# Long.MAX_VALUE. This allows KSQL to pause processing if the underlying
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

Page last revised on: {{ git_revision_date }}
