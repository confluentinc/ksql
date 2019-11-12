---
layout: page
title: Collections
tagline:  Streams and tables in ksqlDB
description: Learn about durable collections of events in ksqlDB
keywords: ksqlDB, events, collection, stream, table
---

Although an individual [event](../events.md) represents vitally important
information, events are more useful when grouped together with related events.
ksqlDB supports durable collections of events that are  replicated across
multiple servers. This means that, similar to replicated
[Postgres](https://www.postgresql.org/), if there is a recoverable failure
of some of the servers, your events remain permanently stored. ksqlDB can
do this by storing data in {{ site.aktm }} topics.

Collections are sharded and can grow to arbitrary sizes by adding more
{{ site.ak }} brokers to a ksqlDB cluster. This sharding is more commonly
known as *partitioning* in {{ site.ak }} and is the term that ksqlDB uses
as well. Events with the same key are stored on the same partition. For more
information, see
[Partition Data to Enable Joins](../../developer-guide/partition-data.md).

ksqlDB offers multiple abstractions for storing events. Although a single
event is immutable, collections of events can model either immutability or
mutability to represent change over time. Also, you can derive new collections
from existing collections. Derived collections are kept up to date in real
time, which is the heart of stream processing.

Collections are represented as a series of rows and columns that have a
defined schema. Only data that conforms to the schema can be added to the
collection.

ksqlDB supports two abstractions for representing collections:
[streams](streams.md) and [tables](tables.md).