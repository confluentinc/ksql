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
SHOW | LIST TOPICS [EXTENDED];
```

Description
-----------

SHOW TOPICS lists the available topics in the Kafka cluster that ksqlDB is
configured to connect to (default setting for `bootstrap.servers`:
`localhost:9092`). SHOW TOPICS EXTENDED also displays consumer groups
and their active consumer counts.

Example
-------

TODO: example


Page last revised on: {{ git_revision_date }}
