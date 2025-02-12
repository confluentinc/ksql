---
layout: page
title: ASSERT TOPIC
tagline:  ksqlDB ASSERT TOPIC statement syntax
description: Assert the existence of a topic.
keywords: ksqlDB, assert, topic
---

## Synopsis

```sql
ASSERT (NOT EXISTS)? TOPIC topicName (WITH properties)? (TIMEOUT timeout); 
```

## Description

Asserts that a topic exists or does not exist.

If properties are specified in an `ASSERT TOPIC` statement, then ksqlDB will also check that the property values match.
The only properties that will be checked are `PARTITIONS` and `REPLICAS`. Trying to check any other
properties will result in the assertion failing.

Properties will not be checked in an `ASSERT NOT EXISTS` statement.

The `TIMEOUT` clause specifies the amount of time to wait for the assertion to succeed before failing.
If the `TIMEOUT` clause is not present, then ksqlDB will use the timeout specified by the server
configuration `ksql.assert.topic.default.timeout.ms`, which is 1000 ms by default. 

If the assertion fails, then an error will be returned.