---
layout: page
title: Data definition
tagline: Use DDL to structure data 
description: How to use DDL to structure data in ksqlDB
keywords: ksqldb, sql, ddl
---

This section covers how you create the structures that store your events.
ksqlDB abstracts events as rows with columns and stores them in streams
and tables.

## Rows and columns

Streams and tables help you model collections of events that accrete over time.
Both are represented as a series of rows and columns with a schema, much like a
relational database table. Rows represent individual events. Columns represent
the attributes of those events.

Each column has a data type. The data type limits the span of permissible values
that you can assign. For example, if a column is declared as type `INT`, it can't
be assigned the value of string `'foo'`.

In contrast to relational database tables, the columns of a row in ksqlDB are
divided into _key_ and _value_ columns. The key columns control which partition
a row resides in. The value columns, by convention, store the main data of
interest. Controlling the key columns is useful for manipulating the underlying
data locality, and enables you to integrate with the wider {{ site.ak }}
ecosystem, which uses the same key/value data model. By default, a column is a
value column. Marking a column as a `(PRIMARY) KEY` makes it a key column.

!!! important
    You must declare a PRIMARY KEY when you create a table on a {{ site.ak }}
    topic.

Internally, each row is backed by a [Kafka record](../../../overview/apache-kafka-primer/#records).
In {{ site.ak }}, the key and value parts of a record are
[serialized](../../../overview/apache-kafka-primer/#serializers) independently.
ksqlDB enables you to exercise this same flexibility and builds on the semantics
of {{ site.ak }} records, rather than hiding them.

There is no theoretical limit on the number of columns in a stream or table.
In practice, the limit is determined by the maximum message size that {{ site.ak }}
can store and the resources dedicated to ksqlDB.

## Streams

A stream is a partitioned, immutable, append-only collection that represents a
series of historical facts. For example, the rows of a stream could model a
sequence of financial transactions, like "Alice sent $100 to Bob", followed by
"Charlie sent $50 to Bob".

Once a row is inserted into a stream, it can never change. New rows can be
appended at the end of the stream, but existing rows can never be updated or
deleted.

Each row is stored in a particular partition. Every row, implicitly or explicitly,
has a key that represents its identity. All rows with the same key reside in the
same partition.

To create a stream, use the `CREATE STREAM` command. The following example
statement specifies a name for the new stream, the names of the columns, and
the data type of each column.

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

This creates a new stream named `s1` with three columns: `k`, `v1`, and `v2`.
The column `k` is designated as the key of this stream, which controls the
partition that each row is stored in. When the data is stored, the value
portion of each row's underlying {{ site.ak }} record is serialized in the
JSON format.

Under the hood, each stream corresponds to a [Kafka topic](../../../overview/apache-kafka-primer/#topics)
with a registered schema. If the backing topic for a stream doesn't exist when
you declare it, ksqlDB creates it on your behalf, as shown in the previous
example statement.

You can also declare a stream on top of an existing topic. When you do that,
ksqlDB simply registers its associated schema. If topic `s2` already exists,
the following statement register a new stream over it:

```sql
CREATE STREAM s2 (
    k1 VARCHAR KEY,
    v1 VARCHAR
) WITH (
    kafka_topic = 's2',
    value_format = 'json'
);
```

!!! tip
    When you create a stream on an existing topic, you don't need to declare
    the number of partitions for the topic. ksqlDB infers the partition count
    from the existing topic.

## Tables

A table is a mutable, partitioned collection that models change over time. In
contrast with a stream, which represents a historical sequence of events, a
table represents what is true as of "now". For example, you might use a table
to model the locations where someone has lived as a stream: first Miami, then
New York, then London, and so forth.

Tables work by leveraging the keys of each row. If a sequence of rows shares a
key, the last row for a given key represents the most up-to-date information
for that key's identity. A background process periodically runs and deletes all
but the newest rows for each key.

Syntactically, declaring a table is similar to declaring a stream. The following
example statement declares a `current_location` table that has a key field 
named `person`.

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

As with a stream, you can declare a table directly on top of an existing
{{ site.ak }} topic by omitting the number of partitions in the `WITH` clause.

## Keys

You can mark a column with the `KEY` keyword to indicate that it's a key
column. Key columns constitute the key portion of the row's underlying
{{ site.ak }} record. Only streams can mark columns as keys, and it's optional
for them to do. Tables must use the `PRIMARY KEY` constraint instead.

In the following example statement, `k1`'s data is stored in the key portion of
the row, and `v1`'s data is stored in the value.

```sql
CREATE STREAM s3 (
    k1 VARCHAR KEY,
    v1 VARCHAR
) WITH (
    kafka_topic = 's3',
    value_format = 'json'
);
```

The ability to declare key columns explicitly is especially useful when you're
creating a stream over an existing topic. If ksqlDB can't infer what data is in
the key of the underlying {{ site.ak }} record, it must perform a repartition
of the rows internally. If you're not sure what data is in the key or you simply
don't need it, you can omit the `KEY` keyword.

## Headers

Starting in ksqlDB 0.24, you can mark a column with `HEADERS` or `HEADER('<key>')` to
indicate that it is populated by the header field of the underlying {{ site.ak }} record.
A column marked with `HEADERS` must have the type `ARRAY<STRUCT<key STRING, value BYTES>>`
and contains the full list of the {{ site.ak }} record's header keys and values.

A column marked with `HEADER('<key>')` must have the type `BYTES` and contains the last
header that matches the key. If the {{ site.ak }} record does not contain a header with the
specified key, then that column will be populated by `NULL`.

In the following example statement, `k1`'s data is stored in the key portion of
the row, `v1`'s data is stored in the value and `h1`'s data is stored in the header.

```sql
CREATE STREAM s4 (
    k1 VARCHAR KEY,
    v1 VARCHAR,
    h1 ARRAY<STRUCT<key STRING, value BYTES>> HEADERS
) WITH (
    kafka_topic = 's3',
    value_format = 'json'
);
```

Header columns can be used in queries just like any other column, but they are read-only.
Furthermore, it is not possible to write into any sink topic headers. Any sink topic columns created
from querying a `HEADERS` column in the source topic will be a value column rather than a `HEADERS`
column representing the sink topic's headers.

```sql
CREATE STREAM s4 (
    k1 VARCHAR KEY,
    v1 VARCHAR,
    h1 BYTES HEADER('abc')
) WITH (
    kafka_topic = 's4',
    value_format = 'json'
);

-- This will create a stream, s5 that contains one value field, decoded, which does not represent the headers in the sink topic.
CREATE STREAM s5 AS SELECT FROM_BYTES(h1, 'ascii') AS decoded FROM s4;

-- This will throw an error
INSERT INTO s4 VALUES ('abc', 'def', ARRAY[]);
```

## Default values

If a column is declared in a schema, but no attribute is present in the
underlying {{ site.ak }} record, the value for the row's column is populated as
`null`.

## Pseudocolumns

A pseudocolumn is a column that's automatically populated by ksqlDB and contains
meta-information that can be inferred about the row at creation time. By default,
pseudocolumns aren't returned when selecting all columns with the star (`*`)
special character. You must select them explicitly, as shown in the following
example statement.

```sql
SELECT ROWTIME, * FROM s1 EMIT CHANGES;
```

The following table lists all pseudocolumns.

| Pseudocolumn   | Meaning                        |
|----------------|--------------------------------|
| `HEADERS`      | Columns that are populated by the {{ site.ak }} record's header. |
| `ROWOFFSET`    | The offset of the source record. |
| `ROWPARTITION` | The partition of the source record. |
| `ROWTIME`      | Row timestamp, inferred from the underlying {{ site.ak }} record if not overridden. |

You can't create additional pseudocolumns beyond these.

## Constraints

Although data types help limit the range of values that can be accepted by
ksqlDB, sometimes it's useful to have more sophisticated restrictions.
_Constraints_ enable you to exercise this type of logic directly in your schema.

### Primary key constraints

In a relational database, a primary key indicates that a column will be used as
a unique identifier for all rows in a table. If you have a table that has a row
with primary key `5`, you can't insert another row whose primary key is also `5`.

ksqlDB uses primary keys in a similar way, but there are a few differences,
because ksqlDB is a streaming database, not a relational database.

- Only tables can have primary keys. Streams do not support them.
- Adding multiple rows to a table with the same primary key doesn't cause the
  subsequent rows to be rejected.
  
The reason for both of these behaviors is the same: the purpose of tables is to
model change of particular identities, but streams are used to accrete facts.
When you insert multiple rows with the same primary key into a table, ksqlDB
interprets these rows as changes to a single identity.

Primary keys can't be null, and they must be used in all declared tables. In
the following example statement, `id` acts as the primary key for table `users`:

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

### Not-null constraints

A _not-null constraint_ designates that a column can't contain a null value.
ksqlDB doesn't support this constraint, but you can track its progress in
[GitHub issue 4436](https://github.com/confluentinc/ksql/issues/4436).
