---
layout: page
title: ksqlDB Processing Log
tagline: Debug your SQL statements in ksqlDB
description: Learn how to debug your ksqlDB applications by using the processing log
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/reference/processing-log.html';
</script>

# Processing log

ksqlDB emits a log of record processing events, named the "processing log",
to help you with debugging your SQL queries. As ksqlDB executes a query,
it writes records to the processing log that detail how it processes
each row, including any errors it encounters along the way.

Log entries are written with a hierarchical name that you can use to track
back to the query execution plan. Log entries are structured events, so in
addition to using them to help you debug, they should be easy to consume from
downstream applications and from ksqlDB itself. In fact, ksqlDB supports
writing the processing log to {{ site.ak }} and consuming it as ksqlDB stream.

!!! important
	The processing log is not for server logging, but rather for per-record
    logging on ksqlDB applications. If you want to configure an {{ site.ak }} appender
    for the server logs, assign the `log4j.appender.kafka_appender.Topic`
    and `log4j.logger.io.confluent.ksql` configuration settings in the ksqlDB
    Server config file. For more information, see
    [ksqlDB Server Log Settings](../../operate-and-deploy/installation/server-config/).

## Logger Names

The logger name identifies the logger that emits a log record. Logger
names are hierarchical. The logger name always has the prefix
`processing.<query-id>`, where `query-id` refers to the SQL query ID,
which you can see with statements like `LIST QUERIES;`.

Loggers for a given query are organized into a hierarchy according to the step
in the execution plan that uses the logger. You can configure the log level by
using a prefix of the logger name to set the level for all loggers under that
prefix. The logger name for a given step is included in the execution plan:

```
Execution plan
--------------
> [ SINK ] | Schema: [VIEWTIME : BIGINT, KSQL_COL_1 : VARCHAR, KSQL_COL_2 : VARCHAR] | Logger: processing.CSAS_PAGEVIEWS_UPPER_0.PAGEVIEWS_UPPER
     > [ PROJECT ] | Schema: [VIEWTIME : BIGINT, KSQL_COL_1 : VARCHAR, KSQL_COL_2 : VARCHAR] | Logger: processing.CSAS_PAGEVIEWS_UPPER_0.Project
         > [ SOURCE ] | Schema: [PAGEVIEWS_ORIGINAL.ROWTIME : BIGINT, PAGEVIEWS_ORIGINAL.VIEWTIME : BIGINT, PAGEVIEWS_ORIGINAL.USERID : VARCHAR, PAGEVIEWS_ORIGINAL.PAGEID : VARCHAR] | Logger: processing.CSAS_PAGEVIEWS_UPPER_0.KsqlTopic
```

## Configuration Using Log4J

Internally, the log uses Log4J to write entries, so you can configure it
just like you configure the normal ksqlDB log.

!!! important
    ksqlDB logs only error messages and doesn't use the log level from the
    log4.properties file, which means that you can't change the log level of
    the processing log.

- For local deployments, edit the
[log4j.properties](https://github.com/confluentinc/ksql/blob/master/config/log4j.properties)
config file to assign Log4J properties.
- For Docker deployments, set the corresponding environment variables. For more
  information, see
  [Configure ksqlDB with Docker](/operate-and-deploy/installation/install-ksqldb-with-docker/)
  and [Configure Docker Logging](https://docs.confluent.io/platform/current/installation/docker/operations/logging.html#log4j-log-levels).

All entries are written under the `processing` logger hierarchy.

Restart the ksqlDB Server for your configuration changes to take effect.

The following example shows how to configure the processing log to emit all
events at ERROR level or higher to an appender that writes to `stdout`:

```properties
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=[%d] %p %m (%c:%L)%n
log4j.logger.processing=ERROR, stdout
log4j.additivity.processing=false
```

If you're using a Docker deployment, set the following environment variables
in your docker-compose.yml:

```properties
environment:
    # --- ksqlDB Server log config ---
    KSQL_LOG4J_ROOT_LOGLEVEL: "ERROR"
    KSQL_LOG4J_LOGGERS: "org.apache.kafka.connect.runtime.rest=WARN,org.reflections=ERROR"
    # --- ksqlDB processing log config ---
    KSQL_LOG4J_PROCESSING_LOG_BROKERLIST: kafka:29092
    KSQL_LOG4J_PROCESSING_LOG_TOPIC: <ksql-processing-log-topic-name>
    KSQL_KSQL_LOGGING_PROCESSING_TOPIC_NAME: <ksql-processing-log-topic-name>
    KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: "true"
    KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: "true"
```

For more information, see
[Create a log4J configuration](https://developer.confluent.io/tutorials/handling-deserialization-errors/ksql.html#create-a-log4j-configuration)
in the
[How to handle deserialization errors](https://developer.confluent.io/tutorials/handling-deserialization-errors/ksql.html)
tutorial.

For the full Docker example configuration, see the
[Multi-node ksqlDB and Kafka Connect clusters](https://github.com/confluentinc/demo-scene/blob/master/multi-cluster-connect-and-ksql/docker-compose.yml)
demo.

## Processing Log Security

By default, the record-processing log doesn't log any actual row data.
To help you debug, you can enable including row data in log records by
setting the ksqlDB property `ksql.logging.processing.rows.include` to
`true`.

!!! important
    In {{ site.ccloud }}, `ksql.logging.processing.rows.include` is set
    to `true`, so the default behavior is to include row data in the
    processing log. It can be configured manually when provisioning the cluster in 
    {{ site.ccloud }} by toggling **Hide row data in processing log** when provisioning 
    in the UI, or by setting the `log-exclude-rows` flag in the CLI.

When `ksql.logging.processing.rows.include` is set to `true`, ensure that the
log is configured to write to a destination where it is safe to write the data
being processed. It's also important to set `log4j.additivity.processing=false`
as shown in the previous example, to ensure that processing log events are not
forwarded to appenders configured for the other ksqlDB loggers.

You can disable the log completely by setting the level to `OFF` in the
[log4j.properties](https://github.com/confluentinc/ksql/blob/master/config/log4j.properties)
file:

```properties
log4j.logger.processing=OFF
```

!!! note
    To enable security for the ksqlDB Processing Log, assign Log4J properties
    as shown in
    [log4j-secure.properties](https://github.com/confluentinc/cp-demo/blob/master/scripts/helper/log4j-secure.properties).

## Log Schema

Log entries are structured and have the following schema:

logger (STRING)

:   The name of the logger that wrote the log entry.

level (STRING)

:   The log level that the entry was logged with.

time (LONG)

:   The time that the entry was logged.

message (STRUCT)

:   The log entry itself.

message.type (INT)

:   An int that describes the type of the log message. Currently, the
    following types are defined: 0 (DESERIALIZATION_ERROR), 1
    (RECORD_PROCESSING_ERROR), 2 (PRODUCTION_ERROR), 3 (SERIALIZATION_ERROR).

message.deserializationError (STRUCT)

:   The contents of a message with type 0 (DESERIALIZATION_ERROR).
    Logged when a deserializer fails to deserialize an {{ site.ak }} record.

message.deserializationError.target (STRING)

:   Either "key" or "value" representing the target component of the row that
    failed to deserialize.

message.deserializationError.errorMessage (STRING)

:   A string containing a human-readable error message detailing the
    error encountered.

message.deserializationError.recordB64 (STRING)

:   The {{ site.ak }} record, encoded in Base64.

message.deserializationError.cause (LIST<STRING>)

:   A list of strings containing human-readable error messages
    for the chain of exceptions that caused the main error.

message.deserializationError.topic (STRING)

:   The {{ site.ak }} topic of the record for which deserialization failed.

message.recordProcessingError (STRUCT)

:   The contents of a record with type 1 (RECORD_PROCESSING_ERROR).
    Logged when ksqlDB hits an error when processing a record, for
    example, an unexpected null value when evaluating an operator in a
    SELECT clause.

message.recordProcessingError.errorMessage (STRING)

:   A string containing a human-readable error message detailing the
    error encountered.

message.recordProcessingError.record (STRING)

:   The SQL record, serialized as a JSON string.

message.recordProcessingError.cause (LIST<STRING>)

:   A list of strings containing human-readable error messages
    for the chain of exceptions that caused the main error.

message.productionError (STRUCT)

:   The contents of a message with type 2 (PRODUCTION_ERROR). Logged
    when a producer fails to publish an {{ site.ak }} record.

message.productionError.errorMessage (STRING)

:   A string containing a human-readable error message detailing the
    error encountered.
    
message.serializationError (STRUCT)

:   The contents of a message with type 3 (SERIALIZATION_ERROR).
    Logged when a serializer fails to serialize a ksqlDB row.
    
message.serializationError.target (STRING)

:   Either "key" or "value" representing the target component of the row that
    failed to serialize.

message.serializationError.errorMessage (STRING)

:   A string containing a human-readable error message detailing the
    error encountered.

message.serializationError.record (STRING)

:   The ksqlDB row, as a human-readable string.

message.serializationError.cause (LIST<STRING>)

:   A list of strings containing human-readable error messages
    for the chain of exceptions that caused the main error.

message.serializationError.topic (STRING)

:   The {{ site.ak }} topic to which the ksqlDB row that failed to serialize
    would have been produced.

message.kafkaStreamsThreadError.message (STRING)

:   A string containing a human-readable error message detailing the
    error encountered.

message.kafkaStreamsThreadError.name (STRING)

:   A string containing the thread name.

message.kafkaStreamsThreadError.cause (LIST<STRING>)

:   A list of strings containing human-readable error messages
    for the chain of exceptions that caused the main error.

## Log Stream

We recommend configuring the query processing log to write entries back
to {{ site.ak }}. This way, you can configure ksqlDB to set up a stream over the
topic automatically.

To log to Kafka, set up a Kafka appender and a special layout for
formatting the log entries as JSON:

```properties
log4j.appender.kafka_appender=org.apache.kafka.log4jappender.KafkaLog4jAppender
log4j.appender.kafka_appender.layout=io.confluent.common.logging.log4j.StructuredJsonLayout
log4j.appender.kafka_appender.BrokerList=<list of kafka brokers>
log4j.appender.kafka_appender.Topic=<kafka topic>
log4j.logger.processing=ERROR, kafka_appender
```

The `list of kafka brokers` setting is a comma-separated list of brokers
in the Kafka cluster, and `kafka topic` is the name of the Kafka topic
to log to.

To have ksqlDB set up the log topic automatically at startup, include the
following in your ksqlDB properties file:

```properties
ksql.logging.processing.topic.auto.create=true
ksql.logging.processing.topic.name=<kafka topic>  # defaults to <ksql service id>ksql_processing_log
```

The replication factor and partition count are configurable using the
`ksql.logging.processing.topic.replication.factor` and
`ksql.logging.processing.topic.partitions` properties, respectively.

If `ksql.logging.processing.topic.auto.create` is set to `true`, the
created topic will be deleted as part of cluster termination.

If the `ksql.logging.processing.topic.name` property is not specified,
the processing log topic name will default to
`<ksql service id>processing_log`, where `ksql service id` is the value
from the `ksql.service.id` property. This ensures each ksqlDB cluster gets
its own processing log topic by default.

If you are bringing up a new interactive mode ksqlDB cluster, you can
configure ksqlDB to set up a log stream automatically by including the
following in your ksqlDB properties file:

```properties
ksql.logging.processing.stream.auto.create=true
ksql.logging.processing.stream.name=<stream name>  # defaults to PROCESSING_LOG
```

When you start ksqlDB, you should see the stream in your list of streams:

```
ksql> list streams;

 Stream Name    | Kafka Topic      | Key Format | Value Format | Windowed
-------------------------------------------------------------------------
 PROCESSING_LOG | processing_log   | KAFKA      | JSON         |
-------------------------------------------------------------------------

ksql> describe PROCESSING_LOG;

Name                 : PROCESSING_LOG
Field   | Type
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 LOGGER  | VARCHAR(STRING)
 LEVEL   | VARCHAR(STRING)
 TIME    | BIGINT
 MESSAGE | STRUCT<type INTEGER, deserializationError STRUCT<target VARCHAR(STRING), errorMessage VARCHAR(STRING), recordB64 VARCHAR(STRING), cause ARRAY<VARCHAR(STRING)>, topic VARCHAR(STRING)>, ...> 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

You can query the stream just like you would any other ksqlDB stream.

You can also create the stream yourself by issuing the following DDL statement:

```sql
CREATE STREAM PROCESSING_LOG_STREAM (
   LOGGER STRING,
   LEVEL STRING,
   `TIME` BIGINT,
   MESSAGE STRUCT<
       `TYPE` INTEGER,
       deserializationError STRUCT<
           target STRING,
           errorMessage STRING,
           recordB64 STRING,
           cause ARRAY<STRING>,
          `topic` STRING>,
       recordProcessingError STRUCT<
           errorMessage STRING,
           record STRING,
           cause ARRAY<STRING>>,
       productionError STRUCT<
           errorMessage STRING>,
       serializationError STRUCT<
           target STRING,
           errorMessage STRING,
           record STRING,
           cause ARRAY<STRING>,
          `topic` STRING>>,
       kafkaStreamsError STRUCT<
           threadName STRING,
           errorMessage STRING,
           cause ARRAY<STRING>>)
   WITH (KAFKA_TOPIC='processing_log_topic', VALUE_FORMAT='JSON');
```

!!! note
    Processing log stream auto-creation is supported for
    interactive mode only. Enabling this setting in headless mode causes
    a warning to be printed to the server log.
