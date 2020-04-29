---
layout: page
title: Troubleshoot ksqlDB issues
tagline: When things don't behave as expected
description: Check this list to troubleshoot ksqlDB issues
---

This guide contains troubleshooting information for many ksqlDB issues.

SELECT query does not stop
--------------------------

ksqlDB queries streams continuously and must be stopped explicitly. In the
ksqlDB CLI, use Ctrl+C to stop non-persistent queries, like
`SELECT * FROM myTable EMIT CHANGES`. To stop a persistent query created by
CREATE STREAM AS SELECT or CREATE TABLE AS SELECT, use the TERMINATE statement:
`TERMINATE query_id;`. For more information, see
[TERMINATE](developer-guide/ksqldb-reference/terminate.md).

SELECT query returns no results
-------------------------------

If a ksqlDB query returns no results and the CLI hangs, use Ctrl+C to
stop the query and then review the following topics to diagnose the
issue.

### Verify that the query is based on the correct source topic

Use the `DESCRIBE EXTENDED` statement to view the {{ site.aktm }}
source topic for the stream. For example, if you have a `pageviews`
stream on a Kafka topic named `pageviews`, enter the following statement
in the CLI:

```sql
DESCRIBE EXTENDED PAGEVIEWS;
```

Example output showing the source topic:

```
Name                 : PAGEVIEWS
[...]
Kafka topic          : pageviews (partitions: 1, replication: 1)
```

### Verify that the source topic is populated with data

Your query results may be empty because the Kafka source topic is not
populated with data. Use the {{ site.kcat }} to consume messages and
print a summary.

```bash
docker run --network ksql-troubleshooting_default --tty --interactive --rm \
          confluentinc/cp-kafkacat \
          kafkacat -b kafka:39092 \
          -C -t pageviews \
          -o beginning
```

Example output showing an empty source topic:

```
% Reached end of topic pageviews [0] at offset 0
```

### Verify that new messages are arriving at the source topic

The topic is populated if the {{ site.kcat }} prints messages. However,
it may not be receiving *new* messages. By default, ksqlDB reads from the
end of a topic. A query does not return results if no new messages are
being written to the topic.

To check your query, you can set ksqlDB to read from the beginning of a
topic by assigning the `auto.offset.reset` property to `earliest` using
following statement:

```sql
SET 'auto.offset.reset'='earliest';
```

Example output showing a successful change:

```
Successfully changed local property 'auto.offset.reset' from 'null' to 'earliest'
```

Run your query again. You should get results from the beginning of the
topic. Note that the query may appear to hang if the query reaches the
latest offset and no new messages arrive. The query is simply waiting
for the next message. Use Ctrl+C to stop the query.

### Verify that the query predicate is not too restrictive

If the previous solutions do not resolve the issue, your query may be
filtering out all records because its predicate is too restrictive.
Remove `WHERE` and `HAVING` clauses and run your query again.

### Verify that there are no deserialization errors

ksqlDB doesn't write query results if it's not able to deserialize
message data. Use the `DESCRIBE EXTENDED` statement to check that the
`VALUE_FORMAT` of the stream matches the format of the records that {{
site.kcat }} prints for your topic. Enter the following statement in the
CLI:

```sql
DESCRIBE EXTENDED pageviews;
```

Example output:

```
Name                 : PAGEVIEWS
[...]
Value format         : DELIMITED
```

Example output from {{ site.kcat }} for a DELIMITED topic:

```
1541463125587,User_2,Page_74
1541463125823,User_2,Page_92
1541463125931,User_3,Page_44
% Reached end of topic pageviews [0] at offset 1538
1541463126232,User_1,Page_28
% Reached end of topic pageviews [0] at offset 1539
1541463126637,User_7,Page_64
% Reached end of topic pageviews [0] at offset 1540
1541463126786,User_1,Page_83
^C
```

[Check for message processing failures](#check-for-message-processing-failures)
for serialization errors. For example, if your query specifies JSON for the
`VALUE_FORMAT`, and the underlying topic is not formatted as JSON, you'll see
`JsonParseException` warnings in the ksqlDB Server log. For example:

```
[2018-09-17 12:29:09,929] WARN task [0_10] Skipping record due to deserialization error. topic=[_confluent-metrics] partition=[10] offset=[70] (org.apache.kafka.streams.processor.internals.RecordDeserializer:86)
 org.apache.kafka.common.errors.SerializationException: KsqlJsonDeserializer failed to deserialize data for topic: _confluent-metrics
 Caused by: com.fasterxml.jackson.core.JsonParseException: Unexpected character ((CTRL-CHAR, code 127)): expected a valid value (number, String, array, object, 'true', 'false' or 'null')
```

ksqlDB CLI doesn't connect to ksqlDB Server
---------------------------------------

The following warning may occur when you start the CLI.

```
**************** WARNING ******************
Remote server address may not be valid:
Error issuing GET to KSQL server

Caused by: java.net.SocketException: Connection reset
Caused by: Connection reset
*******************************************
```

A similar error may display when you create a SQL query using the CLI.

```
Error issuing POST to KSQL server
Caused by: java.net.SocketException: Connection reset
Caused by: Connection reset
```

In both cases, the CLI can't connect to the ksqlDB Server. The following topics
may help to diagnose the issue.

### Verify that the ksqlDB CLI is using the correct port

By default, the server listens on port `8088`. See
[Starting the ksqlDB CLI](operate-and-deploy/installation/installing.md#start-the-ksqldb-cli)
for more information.

### Verify that the ksqlDB Server configuration is correct

In the ksqlDB Server configuration file, check that the list of listeners
has the host address and port configured correctly. Search for the
`listeners` setting in the file and verify it is set correctly.

```
listeners=http://0.0.0.0:8088
```

Or if you're running over IPv6:

```
listeners=http://[::]:8088
```

See [Start the ksqlDB Server](operate-and-deploy/installation/installing.md#start-the-ksqldb-server)
for more information.

### Verify that there are no port conflicts

There may be another process running on the port that the ksqlDB Server
listens on. Use the following command to get the Process ID (PID) for
the process running on the port assigned to the ksqlDB Server. The command
below checks the default `8088` port.

```bash
netstat -anv | egrep -w .*8088.*LISTEN
```

Example output:

```
tcp4  0 0  *.8088       *.*    LISTEN      131072 131072    46314      0
```

In this example, `46314` is the PID of the process that is listening on
port `8088`. Run the following command to get information about process
`46314`.

```bash
ps -wwwp 46314
```

Example output:

```
io.confluent.ksql.rest.server.KsqlServerMain ./config/ksql-server.properties
```

If the `KsqlServerMain` process is not shown, a different process has
taken the port that `KsqlServerMain` would normally use. Search for the
`listeners` setting in the ksqlDB Server configuration file and get the
correct port. Start the CLI using the correct port.

See [Start the ksqlDB Server](operate-and-deploy/installation/installing.md#start-the-ksqldb-server)
and [Starting the ksqlDB CLI](operate-and-deploy/installation/installing.md#start-the-ksqldb-cli)
for more information.

Cannot create a stream from the output of a windowed aggregate
--------------------------------------------------------------

ksqlDB doesn't support structured keys, so you can't create a stream
from a windowed aggregate.

ksqlDB doesn't clean up internal topics
---------------------------------------

Make sure that your Kafka cluster is configured with
`delete.topic.enable=true`. See
[deleteTopics](https://docs.confluent.io/{{ site.release }}/clients/javadocs/org/apache/kafka/clients/admin/AdminClient.html)
for more information.

Replicated topic with Avro schema causes errors
-----------------------------------------------

The {{ site.crepfull }} renames topics during replication. If there are
associated Avro schemas, they are not automatically matched with the
renamed topics after replication completes.

Using the PRINT statement for a replicated topic shows that the Avro
schema ID exists in the Schema Registry. ksqlDB can deserialize the Avro
message, however the CREATE STREAM statement fails with a
deserialization error. For example:

```sql
CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH (kafka_topic='pageviews.replica', value_format='AVRO');
```

Example output with a deserialization error:

```
[2018-06-21 19:12:08,135] WARN task [1_6] Skipping record due to deserialization error. topic=[pageviews.replica] partition=[6] offset=[1663] (org.apache.kafka.streams.processor.internals.RecordDeserializer:86)
org.apache.kafka.connect.errors.DataException: pageviews.replica
        at io.confluent.connect.avro.AvroConverter.toConnectData(AvroConverter.java:97)
        at io.confluent.ksql.serde.connect.KsqlConnectDeserializer.deserialize(KsqlConnectDeserializer.java:48)
        at io.confluent.ksql.serde.connect.KsqlConnectDeserializer.deserialize(KsqlConnectDeserializer.java:27)
```

The solution is to register Avro schemas manually against the replicated
subject name for the topic. For example:

```bash
# Original topic name = pageviews
# Replicated topic name = pageviews.replica
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "{\"schema\": $(curl -s http://localhost:8081/subjects/pageviews-value/versions/latest | jq '.schema')}" http://localhost:8081/subjects/pageviews.replica-value/versions
```

Snappy encoded messages don't decompress
-----------------------------------------

If you don't have write access to the `/tmp` directory because it's
set to `noexec`, you need to pass in a directory path for `snappy` that
you have write access to:

```bash
-Dorg.xerial.snappy.tempdir=/path/to/newtmp
```

Check for message processing failures
-------------------------------------

You can check the health of a SQL query by viewing the number of
messages that it has processed and counting how many processing failures
have occurred.

Use the DESCRIBE EXTENDED statement to see `total-messages` and
`failed-messages-per-sec` to get message processing metrics. Note that
the metrics are local to the server where the DESCRIBE statement runs.

```sql
DESCRIBE EXTENDED GOOD_RATINGS;
```

Example output:

```
[...]
Local runtime statistics
------------------------
messages-per-sec:      1.10 total-messages:     2898 last-message: 9/17/18 1:48:47 PM UTC
    failed-messages:         0 failed-messages-per-sec:         0 last-failed: n/a
(Statistics of the local KSQL server interaction with the Kafka topic GOOD_RATINGS)
```

An increasing number of `failed-messages` may indicate problems with
your query. See
[deserialization errors](#verify-that-there-are-no-deserialization-errors)
for typical sources of processing failures.

Check the ksqlDB Server logs
--------------------------

Check the ksqlDB Server logs for errors using the command:

```bash
confluent log ksql-server
```

ksqlDB writes most of its log messages to `stdout` by default.

Look for logs in the default directory at `/usr/local/logs` or in the
`LOG_DIR` that you assigned when starting the CLI. See
[Starting the ksqlDB CLI](operate-and-deploy/installation/installing.md#start-the-ksqldb-cli)
for more information.

If you installed the {{ site.cp }} using RPM or Debian packages,
the logs are in the `/var/log/confluent/` directory.

If you're running ksqlDB using Docker, the output is in the container
logs, for example:

```bash
docker logs <container-id>
docker-compose logs ksql-server
```

Page last revised on: {{ git_revision_date }}
