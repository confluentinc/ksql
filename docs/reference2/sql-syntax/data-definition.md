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



TODO: how rows work w/ kafka: independently serialized. Exercise control over which data goes where. Key/value are two primary namespaces where data goes. Controls parititoning

Each column has a data type. The data type limits the span of permissible values that it can take on. For example, if a column is declared as type `INT`, it cannot take on the value of string `'foo'`.

TODO: columns by default are populated from the value portion of the record. In general, columns are meant to feel like a flat set of values over the record. ksqlDB simplifies this by flattening it, but still allows you to exercise control over which data goes where to integrate with connectors and control partitioning.

There is no theoretical limit on the number of columns in a stream or table. In practice, the limit is determined by the maximum message size that Kafka can store and the resources dedicated to ksqlDB.

## Streams

Streams are partitioned, immutable, append-only collections. They represent a series of historical facts. For example, the rows of a stream could model a sequence of financial transactions, like "Alice sent $100 to Bob”, then “Charlie sent $50 to Bob".

Once a row is inserted into a stream, it can never change. New rows can be appended at the end of the stream, but existing rows can never be updated or deleted.

Each row is stored in a particular partition. Every row, implicitly or explicitly, has a key that represents its identity. All rows with the same key reside in the same partition.

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

This creates a new stream named `s1` with three columns: `k`, `v1`, and `v2`. The column `k` is designated as the key of this stream, which controls which partition each row is stored in. When the data is stored, the value portion of each row's underlying Kafka record will be serialized in the JSON format.

Under the covers, each stream corresponds to a Kafka topic with a registered schema. If the backing topic for a stream doesn't exist when you declare it, ksqlDB creates it on your behalf, as in the example above.

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

Tables are mutable, partitioned collections that model change over time. By contrast to streams, which represent a historical sequence of events, tables represent what is true as of "now". For example, you might use a table to model the locations that someone has lived at as a stream: first Miami, then New York, then London, and so forth.

Tables work by leveraging the keys of each row. If a sequence of rows shares a key, the last row for a given key represents the most up-to-date information for that key's identity. A background process periodically runs and deletes all but the newest rows for each key.

Here is what declaring a table looks like in code. Syntactically, it is almost the same as declaring a stream.

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

As with streams, tables can also be declared directly ontop of an existing Kafka topic. Simply omit the number of partitions in the `WITH` clause.

## Keys

TODO

What they do: extract data out of the key namespace of a Kafka record. Only used for streams. How they differ from primary keys. Optional for streams.

Why they're useful: avoid a repartition

What to do if your key is not set or is in a different format

## Default values

If a column is declared in a schema, but no attribute is present in the underlying Kafka record, the value for the row's column is populated as `null`.

## Pseudocolumns

TODO

## Constraints

Although data types help limit the range of values that can be accepted by ksqlDB, sometimes it is useful to have more sophisticated restrictions. Constraints allow you to exercise that type of logic directly in your schema.

### Primary key constraints

In a relational database, a primary key indicates that a column will be used as a unique identifier for all rows in a table. If you have a table with a row in it who's primary key is `5`, you can't insert another row whose primary key is also `5`.

ksqlDB uses primary keys in a similar way, but there are a few differences because it is an event streaming database, not a relational database.

First, only tables can have primary keys. Streams do not support them. Second, adding multiple rows to a table with the same primary key doesn't cause the subsequent rows to be rejected. The reason for both of these behaviors is the same: the purpose of tables are to model change of particular identities, whereas streams are used to accrete facts. When you insert multiple rows to a table with the same primary key, ksqlDB inteprets those rows as changes to a single identity.

Primary keys cannot be null, and they must be used in all declared tables. In this example, `id` acts as the primary key for table `users`:

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

TODO: What to do if your key is not set or is in a different format



### Not-null constraints

A not-null constraint designates that a column cannot contain a null value. ksqlDB doesn't yet support this constraint, but you can track the progress towards it in [GitHub issue 4436](https://github.com/confluentinc/ksql/issues/4436).
