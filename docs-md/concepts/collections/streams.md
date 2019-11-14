---
layout: page
title: Streams
tagline: Stream collections in ksqlDB
description: Learn about streams of events in ksqlDB.
keywords: ksqldb, collection, stream
---

A stream is a durable, partitioned sequence of immutable events. When a new
event is added a stream, it's appended to the partition that its key belongs
to. Streams are useful for modeling a historical sequence of activity. For
example, you might use a stream to model a series of customer purchases or a
sequence of readings from a sensor. Under the hood, streams are simply stored
as {{ site.aktm }} topics with an enforced schema. You can create a stream from
scratch or declare a stream on top of an existing {{ site.ak }} topic. In both
cases, you can specify a variety of configuration options.

Create a stream from scratch
------------------------------

When you create a stream from scratch, a backing {{ site.ak }} topic is created
automatically. Use the CREATE STREAM statement to create a stream from scratch,
and give it a name, schema, and configuration options. The following statement
registers a `publications` stream on a topic named `publication_events`. Events
in the `publications` stream are distributed over 3 partitions, are keyed on
the `author` column, and are serialized in the Avro format.

```sql
CREATE STREAM publications (author VARCHAR, title VARCHAR)
    WITH (kafka_topic = 'publication_events',
          partitions = 3,
          key = 'author',
          value_format = 'avro');
```

In this example, a new stream named `publications` is created with two columns:
`author` and `title`. Both are of type `VARCHAR`. ksqlDB automatically creates
an underlying `publication_events` topic that you can access freely. The topic
has 3 partitions, and any new events that are appended to the stream are hashed
according to the value of the `author` column. Because {{ site.ak }} can store
data in a variety of formats, we let ksqlDB know that we want the value portion
of each row stored in the Avro format. You can use a variety of configuration
options in the final `WITH` clause.

!!! note
    If you create a stream from scratch, you must supply the number of
    partitions.

Create a stream over an existing Kafka topic
--------------------------------------------

You can also create a stream on top of an existing {{ site.ak }} topic.
Internally, ksqlDB simply registers the topic with the provided schema
and doesn't create anything new. 

```sql
CREATE STREAM publications (author VARCHAR, title VARCHAR)
    WITH (kafka_topic = 'publication_events',
          value_format = 'avro');
```

Because the topic already exists, you can't specify the number of partitions.
The key shouldn't be set here either, because any data that already exists in
the same topic has a given key.

If an underlying event in the {{ site.ak }} topic doesn’t conform to the given
stream schema, the event is discarded at read-time.

Page last revised on: {{ git_revision_date }}