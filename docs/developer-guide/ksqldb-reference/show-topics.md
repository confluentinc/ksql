---
layout: page
title: SHOW TOPICS
tagline:  ksqlDB SHOW TOPICS statement
description: Syntax for the SHOW TOPICS statement in ksqlDB
keywords: ksqlDB, list, topic
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/show-topics.html';
</script>

SHOW TOPICS
===========

Synopsis
--------

```sql
SHOW | LIST [ALL] TOPICS [EXTENDED];
```

Description
-----------

SHOW TOPICS lists the available topics in the {{ site.ak }} cluster that ksqlDB is
configured to connect to (default setting for `bootstrap.servers`:
`localhost:9092`). SHOW TOPICS EXTENDED also displays consumer groups
and their active consumer counts.

`SHOW TOPICS` does not display hidden topics by default, such as:
* KSQL internal topics, like the KSQL command topic or changelog & repartition topics, or
  topics that match any pattern in the `ksql.hidden.topics` configuration.

`SHOW ALL TOPICS` lists all topics, including hidden topics.

Example
-------

```sql
ksql> SHOW TOPICS;

 Kafka Topic                                                                           | Partitions | Replicas
---------------------------------------------------------------------------------------------------------------
 default_ksql_processing_log                                                           | 1          | 1
 pageviews                                                                             | 1          | 1
 users                                                                                 | 1          | 1
---------------------------------------------------------------------------------------------------------------
```


```sql
ksql> SHOW ALL TOPICS;

 Kafka Topic                                                                           | Partitions | Replicas
--------------------------------------------------------------------------------------------------------------
 _confluent-ksql-default__command_topic                                                | 1          | 1
 _confluent-ksql-default_query_CTAS_USERS_0-Aggregate-Aggregate-Materialize-changelog  | 1          | 1
 _confluent-ksql-default_query_CTAS_USERS_0-Aggregate-GroupBy-repartition              | 1          | 1
 default_ksql_processing_log                                                           | 1          | 1
 pageviews                                                                             | 1          | 1
 users                                                                                 | 1          | 1
--------------------------------------------------------------------------------------------------------------
```
