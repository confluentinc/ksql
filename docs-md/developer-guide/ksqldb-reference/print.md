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
PRINT qualifiedName [FROM BEGINNING] [INTERVAL interval] [LIMIT limit]
```

Description
-----------

Print the contents of Kafka topics to the ksqlDB CLI.

!!! important
	SQL grammar defaults to uppercase formatting. You can use quotations
    (`"`) to print topics that contain lowercase characters.

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
PRINT 'ksql__commands' FROM BEGINNING;
```

Your output should resemble:

```json
    Format:JSON
    {"ROWTIME":1516010696273,"ROWKEY":"\"stream/CLICKSTREAM/create\"","statement":"CREATE STREAM clickstream (_time bigint,time varchar, ip varchar, request varchar, status int, userid int, bytes bigint, agent varchar) with (kafka_topic = 'clickstream', value_format = 'json');","streamsProperties":{}}
    {"ROWTIME":1516010709492,"ROWKEY":"\"table/EVENTS_PER_MIN/create\"","statement":"create table events_per_min as select userid, count(*) as events from clickstream window  TUMBLING (size 10 second) group by userid EMIT CHANGES;","streamsProperties":{}}
    ^CTopic printing ceased
```

Page last revised on: {{ git_revision_date }}
