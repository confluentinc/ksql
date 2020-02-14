---
layout: page
title: PRINT
tagline:  ksqlDB PRINT statement
description: Syntax for the PRINT statement in ksqlDB
keywords: ksqlDB, print, topic
---

PRINT 
=====

Synopsis
--------

```sql
PRINT topicName [FROM BEGINNING] [INTERVAL interval] [LIMIT limit]
```

Description
-----------

Print the contents of Kafka topics to the ksqlDB CLI.

The PRINT statement supports the following properties:

|     Property      |                                                   Description                                                    |
| ----------------- | ---------------------------------------------------------------------------------------------------------------- |
| FROM BEGINNING    | Print starting with the first message in the topic. If not specified, PRINT starts with the most recent message. |
| INTERVAL interval | Print every `interval` th message. The default is 1, meaning that every message is printed.                      |
| LIMIT limit       | Stop printing after `limit` messages. The default value is unlimited, requiring Ctrl+C to terminate the query.   |

Example
-------

The following statement shows how to print all of the records in a topic named
`ksql__commands`.

```sql
PRINT ksql__commands FROM BEGINNING;
```

ksqlDB attempts to determine the format of the data in the topic and outputs what it thinks are
the key and value formats at the top of the output.

!!! note
   Attempting to determine a data format from only the serialized bytes is not an exact science!
   For example, it is not possible to distinguish between serialized `BIGINT` and `DOUBLE` values,
   because they both occupy eight bytes. Short strings can also be mistaken for serialized numbers.
   Where it appears different records are using different formats ksqlDB will state the format is `MIXED`.

Your output should resemble:

```
    Key format: KAFKA (INTEGER)
    Value format: JSON
    rowtime: 12/21/18 23:58:42 PM PSD, key: stream/CLICKSTREAM/create, value: {statement":"CREATE STREAM clickstream (_time bigint,time varchar, ip varchar, request varchar, status int, userid int, bytes bigint, agent varchar) with (kafka_topic = 'clickstream', value_format = 'json');","streamsProperties":{}}
    rowtime: 12/21/18 23:58:42 PM PSD, key: table/EVENTS_PER_MIN/create, value: {"statement":"create table events_per_min as select userid, count(*) as events from clickstream window  TUMBLING (size 10 second) group by userid EMIT CHANGES;","streamsProperties":{}}
    ^CTopic printing ceased
```



Page last revised on: {{ git_revision_date }}
