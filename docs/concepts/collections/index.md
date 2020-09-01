---
layout: page
title: Collections
tagline:  Streams and tables in ksqlDB
description: Learn about durable collections of events in ksqlDB
keywords: ksqlDB, events, collection, stream, table, shard, distributed, partition
---

Although an individual [event](../events.md) represents vitally important
information, events are more useful when stored together with related events.
ksqlDB supports durable collections of events that are  replicated across
multiple servers. This means that, similar to replicated
[Postgres](https://www.postgresql.org/), if there is a recoverable failure
of some of the servers, your events remain permanently stored. ksqlDB can
do this by storing data in {{ site.aktm }} topics.

Collections are distributed and can grow to arbitrary sizes by adding more
brokers to the {{ site.ak }} cluster and more ksqlDB servers to the ksqlDB
cluster. Distributing collections, sometimes called "sharding", is more commonly
known as *partitioning* in {{ site.ak }} and is the term that ksqlDB uses
as well. Events with the same key are stored on the same partition. For more
information, see
[Partition Data to Enable Joins](../../developer-guide/joins/partition-data.md).

Collections provide durable storage for sequences of events. ksqlDB offers
multiple abstractions for storing events. Although a single event is immutable,
collections of events can model either immutability or mutability to represent
change over time. Also, you can derive new collections from existing
collections. Derived collections are kept up to date in real time, which is the
heart of stream processing. For more information, see
[Materialized Views](../materialized-views.md).

Collections are represented as a series of rows and columns that have a
defined schema. Only data that conforms to the schema can be added to the
collection.

ksqlDB supports two abstractions for representing collections:
streams and tables. Both operate under a simple key/value model.

Streams
-------

Streams are immutable, append-only collections. They're useful for representing
a series of historical facts. Adding multiple events that have the same key
means that the events are simply appended to the end of the stream.

Tables
------

Tables are mutable collections. Adding multiple events that have the same key
means the table keeps only the value for the last key. They're helpful for
modeling change over time, and they are often used to represent aggregations.

ksqlDB Collections and {{ site.ak }} Topics
-------------------------------------------

Because ksqlDB leverages {{ site.ak }} for its storage layer, creating a new
collection equates to defining a stream or a table over a {{ site.ak }} topic.
You can declare a collection over an existing topic, or you can create a new
topic for the collection at declaration time.

Use the [CREATE STREAM](../../developer-guide/ksqldb-reference/create-stream.md)
and [CREATE TABLE](../../developer-guide/create-a-table.md) statements to 
register streams and tables over {{ site.ak }} topics.

Insert Events
-------------

Use the INSERT INTO VALUES statement to insert events into an existing stream
or table. for more information, see [Insert Events](inserting-events.md).