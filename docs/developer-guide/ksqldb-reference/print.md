---
layout: page
title: PRINT
tagline:  ksqlDB PRINT statement
description: Syntax for the PRINT statement in ksqlDB
keywords: ksqlDB, print, topic
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/print.html';
</script>

PRINT 
=====

Synopsis
--------

```sql
PRINT topicName [FROM BEGINNING] [INTERVAL | SAMPLE interval] [LIMIT limit]
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
`rider-locations`, created by `CREATE STREAM riderLocations (profileId VARCHAR, latitude DOUBLE, longitude DOUBLE)
WITH (kafka_topic='rider-locations', value_format='json', partitions=1);`.
Note, the topic name has been quoted as it contains the invalid dash character.

```sql
PRINT 'rider-locations' FROM BEGINNING;
```

ksqlDB attempts to determine the format of the data in the topic and outputs what it thinks are
the key and value formats at the top of the output.

!!! note
   Attempting to determine a data format from only the serialized bytes is not an exact science!
   For example, it is not possible to distinguish between serialized `BIGINT` and `DOUBLE` values,
   because they both occupy eight bytes. Short strings can also be mistaken for serialized numbers.
   Where it appears different records are using different formats ksqlDB will state the format is `MIXED`.

The output should resemble:

```
    Key format: JSON or KAFKA_STRING
    Value format: JSON or KAFKA_STRING
    rowtime: 2022/03/18 15:53:22.989 Z, key: 18, value: {"ordertime":1497014222380,"orderid":18,"itemid":"Item_184","address":{"city":"Mountain View","state":"CA","zipcode":94041}}, partition: 0
    rowtime: 2022/03/18 15:53:27.192 Z, key: 18, value: {"ordertime":1497014222380,"orderid":18,"itemid":"Item_184","address":{"city":"Mountain View","state":"CA","zipcode":94041}}, partition: 0
    ^CTopic printing ceased
```

The key format for this topic is `KAFKA_STRING`. However, the `PRINT` command does not know this and
has attempted to determine the format of the key by inspecting the data. It has determined that the
format may be `KAFKA_STRING`, but it could also be `JSON`.

The value format for this topic is `JSON`. However, the `PRINT` command has also determined it could
be `KAFKA_STRING`. This is because `JSON` is serialized as text. Hence you could choose to deserialize
this value data as a `KAFKA_STRING` if you wanted to. However, `JSON` is likely the better option.
