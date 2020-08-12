---
layout: page
title: How to change data
tagline: Use DML to change data 
description: How to use DML to change data in ksqlDB
keywords: ksqldb, sql, dml
---

The [data definition](data-definition.md) section discussed how you create structures to store your events. This section describes how you populate those structures with events, and generally manipulate the events once they are loaded.

## Insert data

- It's just a Kafka topic
- Using the insert statement
- Connector management
- Using the Kafka clients

... And any other way to populate a Kafka topic

- Make sure keys are correct and serialization/schema is right.
- Otherwise will go into processing log.

## Update data

- Unlike standard SQL in this regard
- Streams are immutable, so there is no updating
- But tables are. This is how persistent queries work

## Delete data

- No deletes on streams
- Tombstones for tables
