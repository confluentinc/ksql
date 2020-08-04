---
layout: page
title: Data definition
tagline: Use DDL to structure data 
description: How to use DDL to structure data in ksqlDB
keywords: ksqldb, sql, ddl
---

This section covers how you create the structures that store your events. ksqlDB has two abstractions for that: streams and tables.

## Basics

Streams and tables help you model collections of events that accrete over time. Both are represented as a series of rows and columns with a schema, much like a relational database table. Rows represent individual events. Columns represent attributes of those events.

Each column has a data type. The data type limits the span of permissible values that it can take on. For example, if a column is declared as type `INT`, it cannot take on the value of string `'foo'`.

There is no theoretical limit on the number of columns in a stream or table. In practice, the limit is determined by the maximum message size that Kafka can store and the resources dedicated to ksqlDB.

## Streams

Streams are partitioned, immutable, append-only collections. They represent a series of historical facts. For example, the rows of a stream could model a sequence of financial transactions, like "Alice sent $100 to Bob”, then “Charlie sent $50 to Bob".

Rows in a stream cannot change. New rows can be inserted at the end of the stream, but existing rows can never be updated or deleted.

Streams are partitioned. All rows, implicitly or explicitly, have a key that represents the identity of the row. All rows of the same key are stored in the same partition.

To create a stream, use the `CREATE STREAM` command. In this command, you specify a name for the new stream, the names of the columns, and the data type of each column.

```sql
CREATE STREAM s1 (
    k VARCHAR KEY,
    v1 INT,
    v2 VARCHAR
) WITH (
    kafka_topic = 's1',
    partitions = 3,
    value_format = 'json'
);
```

This creates a new stream named `s1` with three columns: `k`, `v1`, and `v2`. The column `k` is designated as the key of this stream, which controls how partitioning happens. The backing data is serialized in the JSON format.

Under the covers, streams correspond to Kafka topics with registered schemas. Creating a new stream creates a new Kafka topic. All data inserted into a stream resides in a topic on a Kafka broker. In addition to creating streams from scratch, you can also create a stream on an existing topic.

## Tables


## Constraints

- Limit the way that you can put data into a stream/table

### Primary key constraints

- Sole identifier for an entity with multiple rows
- Only used in tables

### Not null constraints

- Not implemented yet, point to GH issue

## Partition data

- Kafka keys control partitioning/sharding
- General requirements
- Different key formats

## Modify streams and tables

- Future work in 0.12