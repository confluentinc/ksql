---
layout: page
title: SHOW TOPICS
tagline:  ksqlDB SHOW TOPICS statement
description: Syntax for the SHOW TOPICS statement in ksqlDB
keywords: ksqlDB, list, topic
---

SHOW TOPICS
===========

Synopsis
--------

```sql
SHOW | LIST [ALL] TOPICS [EXTENDED];
```

Description
-----------

SHOW TOPICS lists the available topics in the Kafka cluster that ksqlDB is
configured to connect to (default setting for `bootstrap.servers`:
`localhost:9092`). SHOW TOPICS EXTENDED also displays consumer groups
and their active consumer counts.

SHOW TOPICS does not display topics considered internal, such as:
* KSQL internal topics, like the KSQL command topic
* Topics found in the `ksql.internal.hidden.topics` configuration

SHOW ALL TOPICS lists all topics, including those considered to be
internal or found in the `ksql.internal.hidden.topics` configuration.

Example
-------

```sql
ksql> SHOW TOPICS;

 Kafka Topic                            | Partitions | Partition Replicas
-------------------------------------------------------------------------
 default_ksql-processing-log            | 1          | 1
 pageviews                              | 1          | 1
 users                                  | 1          | 1
-------------------------------------------------------------------------
```


```sql
ksql> SHOW ALL TOPICS;

 Kafka Topic                            | Partitions | Partition Replicas
-------------------------------------------------------------------------
 _confluent-ksql-default_command_topic  | 1          | 1
 default_ksql-processing-log            | 1          | 1
 pageviews                              | 1          | 1
 users                                  | 1          | 1
-------------------------------------------------------------------------
```
