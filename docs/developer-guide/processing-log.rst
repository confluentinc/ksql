.. _ksql_processing_log:

KSQL Processing Log
-------------------

KSQL emits a log of record processing events, called the processing log, to help developers debug
their KSQL queries. As KSQL executes a query, it writes records to the processing log that detail how
it processes each row, including any errors it encounters along the way.

Log entries are written with a log level, so you can tune the log to emit a verbose trace of every
record processed, to log only errors, or to disable it completely. Log entries are also written with
a hierarchical name that can be correlated back to the query execution plan. This way, you can tune
the log level for specific queries, and even specific steps of a given query.

Log entries are structured events, so in addition to using them to help you debug, they should be
easy to consume from downstream applications and from KSQL itself. In fact, KSQL supports writing
the processing log to Kafka and consuming it as KSQL stream.

Logger Names
============

The logger name identifies the logger that emits a log record. Logger names are hierarchical. The
logger name always has the prefix: ``processing.<query-id>``, where ``query-id`` refers to the KSQL
query ID, which you can see with statements like ``LIST QUERIES;``. Loggers for a given query are organized into a
hierarchy according to the step in the execution plan that uses the logger. You can configure the log level
by using a prefix of the logger name to set the level for all loggers under that prefix.
The logger name for a given step is included in the execution plan:

::

    Execution plan
    --------------
    > [ SINK ] | Schema: [VIEWTIME : BIGINT, KSQL_COL_1 : VARCHAR, KSQL_COL_2 : VARCHAR] | Logger: processing.CSAS_PAGEVIEWS_UPPER_0.PAGEVIEWS_UPPER
    		 > [ PROJECT ] | Schema: [VIEWTIME : BIGINT, KSQL_COL_1 : VARCHAR, KSQL_COL_2 : VARCHAR] | Logger: processing.CSAS_PAGEVIEWS_UPPER_0.Project
    				 > [ SOURCE ] | Schema: [PAGEVIEWS_ORIGINAL.ROWTIME : BIGINT, PAGEVIEWS_ORIGINAL.ROWKEY : VARCHAR, PAGEVIEWS_ORIGINAL.VIEWTIME : BIGINT, PAGEVIEWS_ORIGINAL.USERID : VARCHAR, PAGEVIEWS_ORIGINAL.PAGEID : VARCHAR] | Logger: processing.CSAS_PAGEVIEWS_UPPER_0.KsqlTopic


Configuration Using Log4J
=========================

Internally, the log uses log4j to write entries, so you can configure it just like you would the
normal KSQL log. All entries are written under the logger hierarchy ``processing``. The following
example shows how to configure the processing log to emit all events at ERROR level or higher to
an appender that writes to stdout:

::

     log4j.appender.stdout=org.apache.log4j.ConsoleAppender
     log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
     log4j.appender.stdout.layout.ConversionPattern=[%d] %p %m (%c:%L)%n
     log4j.logger.processing=ERROR, stdout
     log4j.additivity.processing=false

For example, if you want to set the log level to DEBUG for a query named ``CSAS_PAGEVIEWS_UPPER_0``, you
could write the following into your log4j properties file:

::

    log4j.logger.processing=ERROR, stdout
    log4j.logger.processing.CSAS_PAGEVIEWS_UPPER_0=DEBUG
    log4j.additivity.processing=false

Note that the server must be restarted for the config to take effect.

Note On Security
================

Entries in the record-processing log may include data from the rows processed by KSQL.
Be careful to ensure that the log is configured to write to a destination
where it is safe to write the data being processed. It's also important to set
``log4j.additivity.processing=false`` as shown in the previous example, to ensure that processing log
events are not forwarded to appenders configured for the other KSQL loggers.

You can disable the log completely by setting the level to OFF:

::

    log4j.logger.processing=OFF

Log Schema
==========

Log entries are structured and have the following schema:

logger (STRING)
  The name of the logger that wrote the log entry.

level (STRING)
  The log level that the entry was logged with.

time  (LONG)
  The time that the entry was logged.

message (STRUCT)
  The log entry itself.

message.type (INT)
  An int that describes the type of the log message. Currently, the following types are
  defined: 0 (DESERIALIZATION_ERROR), 1 (RECORD_PROCESSING_ERROR), 2 (PRODUCTION_ERROR).

message.deserializationError (STRUCT)
  The contents of a message with type 0 (DESERIALIZATION_ERROR). Logged when a deserializer
  fails to deserialize a Kafka record.

message.deserializationError.errorMessage (STRING)
  A string containing a human-readable error message detailing the error encountered.

message.deserializationError.recordB64 (STRING)
  The Kafka record, encoded in Base64.

message.recordProcessingError (STRUCT)
  The contents of a message with type 1 (RECORD_PROCESSING_ERROR). Logged when KSQL hits
  an error when processing a record, for example, an unexpected null value when evaluating
  an operator in a SELECT clause.

message.recordProcessingError.errorMessage (STRING)
  A string containing a human-readable error message detailing the error encountered.

message.recordProcessingError.record (STRING)
  The KSQL record, serialized as a JSON string.

message.productionError (STRUCT)
  The contents of a message with type 2 (PRODUCTION_ERROR). Logged when a producer fails to
  publish a Kafka record. These messages will only be logged if ``ksql.fail.on.production.error=false``.

message.productionError.errorMessage (STRING)
  A string containing a human-readable error message detailing the error encountered.

Log Stream
==========

We recommend configuring the query processing log to write entries back to
Kafka. This way, you can configure KSQL to set up a stream over the topic automatically.

To log to Kafka, set up a Kafka appender and a special layout for formatting the
log entries as JSON:

::

    log4j.appender.kafka=org.apache.kafka.log4jappender.KafkaLog4jAppender
    log4j.appender.kafka.layout=io.confluent.common.logging.log4j.StructuredJsonLayout
    log4j.appender.kafka.BrokerList=<list of kafka brokers>
    log4j.appender.kafka.Topic=<kafka topic>

The ``list of kafka brokers`` setting is a comma-separated list of brokers in the Kafka cluster, and
``kafka topic`` is the name of the Kafka topic to log to.

To have KSQL set up the log topic automatically at startup, include the following in your KSQL
properties file:

::

    ksql.processing.log.topic.auto.create=on
    ksql.processing.log.topic.name=<kafka topic>  # defaults to processing_log

The replication factor and partition count are configurable
using the ``ksql.processing.log.topic.replication.factor`` and ``ksql.processing.log.topic.partitions`` properties,
respectively.

If you are bringing up a new interactive mode KSQL cluster, you can configure KSQL to set up
a log stream automatically by including the following in your KSQL properties file:

::

    ksql.processing.log.stream.auto.create=on
    ksql.processing.log.stream.name=<stream name>  # defaults to PROCESSING_LOG

When you start KSQL, you should see the stream in your list of streams:

::

    ksql> list streams;

     Stream Name        | Kafka Topic            | Format
    ------------------------------------------------------
     PROCESSING_LOG     | processing_log         | JSON
    ------------------------------------------------------

    ksql> describe PROCESSING_LOG;

    Name                 : PROCESSING_LOG
    Field   | Type
    ---------------------------------------------------------------------------------------------------------------------------
     ROWTIME | BIGINT           (system)
     ROWKEY  | VARCHAR(STRING)  (system)
     LOGGER  | VARCHAR(STRING)
     LEVEL   | VARCHAR(STRING)
     TIME    | BIGINT
     MESSAGE | STRUCT<type INTEGER, deserializationError STRUCT<errorMessage VARCHAR(STRING), recordB64 VARCHAR(STRING)>, ...> 
    ---------------------------------------------------------------------------------------------------------------------------

You can query the stream just like you would any other KSQL stream.

You can also create the stream yourself by issuing the following DDL:

::

    ksql> CREATE STREAM PROCESSING_LOG_STREAM (\
             LOGGER STRING, \
             LEVEL STRING, \
             `TIME` BIGINT, \
             MESSAGE STRUCT< \
                 `TYPE` INTEGER,
                 deserializationError STRUCT< \
                     errorMessage STRING, \
                     recordB64 STRING>, \
                 recordProcessingError STRUCT< \
                     errorMessage STRING, \
                     record STRING>, \
                 productionError STRUCT< \
                     errorMessage STRING>>) \
             WITH (KAFKA_TOPIC='processing_log_topic', VALUE_FORMAT='JSON');

Note that processing log stream auto-creation is supported for interactive mode only. Enabling
this setting in headless mode will cause a warning to be printed to the server log.
