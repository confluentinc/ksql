---
layout: page
title: Tables
tagline: Table collections in ksqlDB
description: Learn about tables of events in ksqlDB.
keywords: ksqldb, collection, table
---

A table is a durable, partitioned collection that models change over time.
It's the mutable counterpart to the immutable [stream](streams.md). By contrast
to streams, which represent a historical sequence of events, tables represent
what is true as of “now”. For example, you might model the locations that
someone has lived at as a stream: first Miami, then New York, then London,
and so forth. You can use a table to roll up this information and tell you
where they live right now. Tables can also be used to materialize a view by
incrementally aggregating a stream of events.

Tables work by leveraging the keys of each event. Keys are used to denote
identity. If a sequence of events shares a key, the last event for a given key
represents the most up-to-date information. Under the hood, ksqlDB uses Kafka’s
notion of a *compacted topic* to make this work. Compaction is a process that
periodically deletes all but the newest events for each key. For more
information, see
[Log Compaction](https://kafka.apache.org/documentation/#compaction).

You can create a table from scratch or declare a table on top of an existing
{{ site.aktm }} topic. You can supply a variety of configuration options. In
either case, the table is not *materialized*, which limits its ability to be
queried. Only tables that are derived from other collections are materialized.
For more information, see [Materialized Views](../materialized-views.md).

Create a table from scratch
---------------------------

When you create a table from scratch, a backing compacted {{ site.ak }} topic
is created automatically. Use the
[CREATE TABLE](../../developer-guide/ksqldb-reference/create-table.md)
statement to create a table from scratch, and give it a name, schema, and
configuration options. The following statement registers a `movies` table on a
topic named `movies`. Events in the `movies` table are distributed over 5
partitions, are keyed on the `title` column, and are serialized in the Avro
format.

```sql
CREATE TABLE movies (title VARCHAR, release_year INT)
    WITH (kafka_topic = 'movies',
          key = 'title'
          partitions = 5,
          value_format = 'avro');
```

In this example, a new table named `movies` is created with two columns:
`title` and `release_year`. ksqlDB automatically creates an underlying `movies`
topic that you can access freely. The topic has 5 partitions, and any new
events that are integrated into the table are hashed according to the value
of the `title` column. Because {{ site.ak }} can store data in a variety of
formats, we let ksqlDB know that we want the value portion of each row stored
in the Avro format. You can use a variety of configuration options in the final
WITH clause.

!!! note
    If you create a table from scratch, you must supply the number of
    partitions.

Create a table over an existing Kafka topic
-------------------------------------------

You can also create a table on top of an existing {{ site.ak }} topic.
Internally, ksqlDB simply registers the topic with the provided schema
and doesn't create anything new. 

```sql
CREATE TABLE movies (title VARCHAR, release_year INT)
    WITH (kafka_topic = 'movies',
          value_format = 'avro');
```

Because the topic already exists, you can't specify the number of partitions.
The key shouldn't be set here either, because any data that already exists in
the same topic has a given key.

If an underlying event in the {{ site.ak }} topic doesn’t conform to the given
table schema, the event is discarded at read-time.

Page last revised on: {{ git_revision_date }}
