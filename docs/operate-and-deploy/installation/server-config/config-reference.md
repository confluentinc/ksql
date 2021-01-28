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

For more information, see [Processing Guarantees](/operate-and-deploy/exactly-once-semantics).

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

### response.http.headers.config

Use to select which HTTP headers are returned in the HTTP response for {{ site.cp }}
components. Specify multiple values in a comma-separated string using the
format ``[action][header name]:[header value]`` where ``[action]`` is one of
the following: ``set``, ``add``, ``setDate``, or ``addDate``. You must use
quotation marks around the header value when the header value contains commas,
for example: 

```properties
response.http.headers.config="add Cache-Control: no-cache, no-store, must-revalidate", add X-XSS-Protection: 1; mode=block, add Strict-Transport-Security: max-age=31536000; includeSubDomains, add X-Content-Type-Options: nosniff  
```

The corresponding environment variable in the
[ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/)
is `KSQL_RESPONSE_HTTP_HEADERS_CONFIG`.

Confluent Control Center Settings
---------------------------------

You can access ksqlDB Server by using {{ site.c3 }}. For more information,
see [Control Center Configuration Reference](https://docs.confluent.io/current/control-center/installation/configuration.html#ksql-settings).


ksqlDB Processing Log Settings
------------------------------

The following configuration settings control the behavior of the
[ksqlDB Processing Log](../../../reference/processing-log.md).

!!! note
    To enable security for the KSQL Processing Log, assign log4j properties
    as shown in
    [log4j-secure.properties](https://github.com/confluentinc/cp-demo/blob/master/scripts/helper/log4j-secure.properties).

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

### ksql.logging.server.rate.limited.response.codes

A list of `code:qps` pairs, to limit the rate of server request
logging.  An example would be "400:10" which would limit 400 error
logs to 10 per second.  This is useful for limiting certain 4XX errors that you
might not want to blow up in the logs.
This setting enables seeing the logs when the request rate is low 
and dropping them when they go over the threshold.
A message will be logged every 5 seconds indicating if the rate limit
is being hit, so an absence of this message means a complete set of logs.

### ksql.logging.server.rate.limited.request.paths

A list of `path:qps` pairs, to limit the rate of server request
logging.  An example would be "/query:10" which would limit pull query
logs to 10 per second. This is useful for requests that are coming in
at a high rate, such as for pull queries.
This setting enables seeing the logs when the request rate is low
and dropping them when they go over the threshold.
A message will be logged every 5 seconds indicating if the rate limit
is being hit, so an absence of this message means a complete set of logs.

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
