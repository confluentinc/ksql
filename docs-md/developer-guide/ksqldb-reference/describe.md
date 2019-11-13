---
layout: page
title: DESCRIBE
tagline:  ksqlDB DESCRIBE statement
description: Syntax for the DESCRIBE statement in ksqlDB
keywords: ksqlDB, describe stream, describe table, metadata
---

DESCRIBE
========

Synopsis
--------

```sql
DESCRIBE [EXTENDED] (stream_name|table_name);
```

Description
-----------

-   DESCRIBE: List the columns in a stream or table along with their
    data type and other attributes.
-   DESCRIBE EXTENDED: Display DESCRIBE information with additional
    runtime statistics, Kafka topic details, and the set of queries that
    populate the table or stream.

Extended descriptions provide the following metrics for the topic
backing the source being described.

|         ksqlDB Metric        |                                                   Description                                                   |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------- |
| consumer-failed-messages     | Total number of failures during message consumption on the server.                                              |
| consumer-messages-per-sec    | The number of messages consumed per second from the topic by the server.                                        |
| consumer-total-message-bytes | Total number of bytes consumed from the topic by the server.                                                    |
| consumer-total-messages      | Total number of messages consumed from the topic by the server.                                                 |
| failed-messages-per-sec      | Number of failures during message consumption (for example, deserialization failures) per second on the server. |
| last-failed                  | Time that the last failure occurred when a message was consumed from the topic by the server.                   |
| last-message                 | Time that the last message was produced to or consumed from the topic by the server.                            |
| messages-per-sec             | Number of messages produced per second into the topic by the server.                                            |
| total-messages               | Total number of messages produced into the topic by the server.                                                 |
| total-message-bytes          | Total number of bytes produced into the topic by the server.                                                    |

Example
-------

The following statement shows how to get a description of a table.

```sql
DESCRIBE ip_sum;
```

Your output should resemble:

```
 Field   | Type
-------------------------------------
 ROWTIME | BIGINT           (system)
 ROWKEY  | VARCHAR(STRING)  (system)
 IP      | VARCHAR(STRING)  (key)
 KBYTES  | BIGINT
-------------------------------------
For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>
```

The following statement shows how to get a description of a table that has
extended information.

```sql
DESCRIBE EXTENDED ip_sum;
```

Your output should resemble:

```
Type                 : TABLE
Key field            : CLICKSTREAM.IP
Timestamp field      : Not set - using <ROWTIME>
Key format           : STRING
Value format         : JSON
Kafka output topic   : IP_SUM (partitions: 4, replication: 1)

 Field   | Type
-------------------------------------
 ROWTIME | BIGINT           (system)
 ROWKEY  | VARCHAR(STRING)  (system)
 IP      | VARCHAR(STRING)  (key)
 KBYTES  | BIGINT
-------------------------------------

Queries that write into this TABLE
-----------------------------------
id:CTAS_IP_SUM - CREATE TABLE IP_SUM as SELECT ip,  sum(bytes)/1024 as kbytes FROM CLICKSTREAM window SESSION (300 second) GROUP BY ip EMIT CHANGES;

For query topology and execution plan run: EXPLAIN <QueryId>; for more information

Local runtime statistics
------------------------
messages-per-sec:      4.41   total-messages:       486     last-message: 12/14/17 4:32:23 PM GMT
  failed-messages:         0      last-failed:       n/a
(Statistics of the local ksqlDB server interaction with the Kafka topic IP_SUM)
```

Page last revised on: {{ git_revision_date }}
