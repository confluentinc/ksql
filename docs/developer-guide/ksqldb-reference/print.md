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

Print the contents of {{ site.ak }} topics to the ksqlDB CLI.

The _topicName_ is case sensitive. Quote the name if it contains invalid characters.
See [Valid Identifiers](/reference/sql/syntax/lexical-structure#identifiers) for more information.

The PRINT statement supports the following properties:

|     Property      |                                                   Description                                                    |
| ----------------- | ---------------------------------------------------------------------------------------------------------------- |
| FROM BEGINNING    | Print starting with the first message in the topic. If not specified, PRINT starts with the most recent message. |
| INTERVAL interval | Print every `interval` th message. The default is 1, meaning that every message is printed.                      |
| LIMIT limit       | Stop printing after `limit` messages. The default value is unlimited, requiring Ctrl+C to terminate the query.   |

Example
-------

The following statement shows how to print all of the records in a topic named
`_confluent-ksql-default__command_topic`, (the default name for the topic ksqlDB stores your submitted command in).
Note, the topic name has been quoted as it contains the invalid dash character.

```sql
PRINT '_confluent-ksql-default__command_topic' FROM BEGINNING;
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
    Key format: JSON or SESSION(KAFKA_STRING) or HOPPING(KAFKA_STRING) or TUMBLING(KAFKA_STRING) or KAFKA_STRING
    Value format: JSON or KAFKA_STRING
    rowtime: 12/21/18 23:58:42 PM PSD, key: stream/CLICKSTREAM/create, value: {statement":"CREATE STREAM clickstream (_time bigint,time varchar, ip varchar, request varchar, status int, userid int, bytes bigint, agent varchar) with (kafka_topic = 'clickstream', value_format = 'json');","streamsProperties":{}}
    rowtime: 12/21/18 23:58:42 PM PSD, key: table/EVENTS_PER_MIN/create, value: {"statement":"create table events_per_min as select userid, count(*) as events from clickstream window  TUMBLING (size 10 second) group by userid EMIT CHANGES;","streamsProperties":{}}
    ^CTopic printing ceased
```

The key format for this topic is `KAFKA_STRING`. However, the `PRINT` command does not know this and
has attempted to determine the format of the key by inspecting the data. It has determined that the
format may be `KAFKA_STRING`, but it could also be `JSON` or a windowed `KAFKA_STRING`.

The value format for this topic is `JSON`. However, the `PRINT` command has also determined it could
be `KAFKA_STRING`. This is because `JSON` is serialized as text. Hence you could choose to deserialize
this value data as a `KAFKA_STRING` if you wanted to. However, `JSON` is likely the better option.
