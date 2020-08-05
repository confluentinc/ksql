---
layout: page
title: Data definition
tagline: Use DDL to structure data 
description: How to use DDL to structure data in ksqlDB
keywords: ksqldb, sql, ddl
---

This section covers how you create the structures that store your events. ksqlDB has two abstractions for that: streams and tables.

## Basics

Streams and tables help you model collections of events that accrete over time. Both are represented as a series of rows and columns with a schema, much like a relational database table. Rows represent individual events. Columns represent the attributes of those events.

Each column has a data type. The data type limits the span of permissible values that it can take on. For example, if a column is declared as type `INT`, it cannot take on the value of string `'foo'`.

There is no theoretical limit on the number of columns in a stream or table. In practice, the limit is determined by the maximum message size that Kafka can store and the resources dedicated to ksqlDB.

## Streams

Streams are partitioned, immutable, append-only collections. They represent a series of historical facts. For example, the rows of a stream could model a sequence of financial transactions, like "Alice sent $100 to Bob”, then “Charlie sent $50 to Bob".

Rows in a stream cannot change. New rows can be inserted at the end of the stream, but existing rows can never be updated or deleted.

Each row is stored in a particular partition. Every row, implicitly or explicitly, has a key that represents its identity. All rows with the same key are stored in the same partition.

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

This creates a new stream named `s1` with three columns: `k`, `v1`, and `v2`. The column `k` is designated as the key of this stream, which controls how partitioning happens. When the data is stored, it will be serialized in the JSON format.

Under the covers, each stream corresponds to Kafka topic with a registered schema. If the backing topic for a stream doesn't exist when you declare it, ksqlDB creates it on your behalf, as in the example above.

You can also declare a stream on top of an existing topic. When you do that, ksqlDB simply registers its associated schema. If topic `s2` already exists, this command will register a new stream over it:

```sql
CREATE STREAM s2 (
    k1 VARCHAR KEY,
    v1 VARCHAR
) WITH (
    kafka_topic = 's2',
    value_format = 'json'
);
```

Note that when you create a stream on an existing topic, you don't need to declare the number of partitions in it. ksqlDB simply infers the partition count from the existing topic.

## Tables

Tables are mutable, partitioned collections that models change over time. By contrast to streams, which represent a historical sequence of events, tables represent what is true as of "now". For example, you might use a table to model the locations that someone has lived at as a stream: first Miami, then New York, then London, and so forth.

Tables work by leveraging the keys of each row. Recall from the streams section that keys denote identity. If a sequence of rows shares a key, the last row for a given key represents the most up-to-date information. A background process runs that periodically deletes all but the newest events for each key.

Here is what that looks like in code.

```sql
CREATE TABLE current_location (
    person VARCHAR PRIMARY KEY,
    location VARCHAR
) WITH (
    kafka_topic = 'current_location',
    partitions = 3,
    value_format = 'json'
);
```

As with streams, tables can also be declared directly ontop of an existing Kafka topic.

## Default values

If a column is declared in a schema, but no attribute is present in the underlying Kafka record, the value for the row's column is populated as `null`.

## Constraints

Although data types help limit the range of values that can be accepted by ksqlDB, sometimes it is useful to have more sophisticated restrictions. Constraints allow you to exercise that type of logic directly in your schema.

### Primary key constraints

A primary key constraint indicates that a column will be used as a unique identifier across all rows in a table. Primary keys cannot be null, and they must be used in all declared tables. In this example, `id` acts as the primary key for table `users`:

```sql
CREATE TABLE users (
    id BIGINT PRIMARY KEY
    name VARCHAR
  ) WITH (
    kafka_topic = 'users',
    partitions = 3,
    value_format = 'json'
  );
```

Because ksqlDB is an event streaming database, primary keys behave somewhat ifferently than compared to a relational database in two ways. First, only tables can have primary keys. Streams do not support them. Second, adding multiple rows to a table with the same primary key doesn't cause the subsequent rows to be rejected. The reason for both of these behaviors is the same: the purpose of tables are to model change of particular identities, whereas streams are used to accrete facts.

## Partitioning

- Kafka keys control partitioning/sharding
- General requirements
- Different key formats

## Modify streams and tables

- Future work in 0.12