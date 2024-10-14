---
layout: page
title: Apache Kafka® primer
tagline: Kafka concepts you need to use ksqlDB
description: Learn the minimum number of Kafka concepts to use ksqlDB effectively
keywords: ksqldb, kafka
---

ksqlDB is a database built specifically for stream processing on {{ site.aktm }}.
Although it's designed to give you a higher-level set of primitives than
{{ site.ak }} has, it's inevitable that all of {{ site.ak }}'s concepts can't be, and
shouldn't be, abstracted away entirely. This section describes the minimum
number of {{ site.ak }} concepts that you need to use ksqlDB effectively.
For more information, consult the official [Apache Kafka documentation](https://kafka.apache.org/documentation/).

## Records

The primary unit of data in {{ site.ak }} is the event. An event models
something that happened in the world at a point in time. In {{ site.ak }},
you represent each event using a data construct known as a record. A record
carries a few different kinds of data in it: key, value, timestamp, topic, partition, offset, and headers.

The _key_ of a record is an arbitrary piece of data that denotes the identity
of the event. If the events are clicks on a web page, a suitable key might be
the ID of the user who did the clicking.

The _value_ is also an arbitrary piece of data that represents the primary data of
interest. The value of a click event probably contains the page that it
happened on, the DOM element that was clicked, and other interesting tidbits
of information.

The _timestamp_ denotes when the event happened. There are a few different "kinds"
of time that can be tracked. These aren’t discussed here, but they’re useful to
[learn about](../../../concepts/time-and-windows-in-ksqldb-queries/#time-semantics) nonetheless.

The _topic_ and _partition_ describe which larger collection and subset of events
this particular event belongs to, and the _offset_ describes its exact position within
that larger collection (more on that below).

Finally, the _headers_ carry arbitrary, user-supplied metadata about the record.

ksqlDB abstracts over some of these pieces of information so you don’t need to
think about them. Others are exposed directly and are an integral part of the
programming model. For example, the fundamental unit of data in ksqlDB is the
_row_. A row is a helpful abstraction over a {{ site.ak }} record. Rows have
columns of two kinds: key columns and value columns. They also carry
pseudocolumns for metadata, like a `timestamp`.

In general, ksqlDB avoids raising up {{ site.ak }}-level implementation details
that don’t contribute to a high-level programming model.

## Topics

Topics are named collections of records. Their purpose is to let you hold
events of mutual interest together. A series of click records might get stored
in a "clicks" topic so that you can access them all in one place. Topics are
append-only. Once you add a record to a topic, you can’t change or delete it
individually.

There are no rules for what kinds of records can be placed into topics. They
don't need to conform to the same structure, relate to the same situation, or
anything like that. The way you manage publication to topics is entirely a
matter of user convention and enforcement.

ksqlDB provides higher-level abstractions over a topic through
_[streams](../reference/sql/data-definition.md#streams)_ and
_[tables](../reference/sql/data-definition.md#tables)_.
A stream or table associates a schema with a {{ site.ak }} topic.
The schema controls the shape of records that are allowed to be stored in the
topic. This kind of static typing makes it easier to understand what sort of
rows are in your topic and generally helps you make fewer mistakes in your
programs that process them.

## Partitions

When a record is placed into a topic, it is placed into a particular partition.
A partition is a totally ordered sequence of records by offset. Topics may have multiple
partitions to make storage and processing more scalable. When you create a
topic, you choose how many partitions it has.

When you append a record to a topic, a partitioning strategy chooses which
partition it is stored in. There are many partitioning strategies. The most common
one is to hash the contents of the record's key against the total number of
partitions. This has the effect of placing all records with the same identity
into the same partition, which is useful because of the strong ordering
guarantees.

The order of the records is tracked by a piece of data known as an offset,
which is set when the record is appended. A record with an offset of _10_ happened
earlier than a record in the same partition with an offset of _20_.

Much of the mechanics here are handled automatically by ksqlDB on your behalf.
When you create a stream or table, you choose the number of partitions for the
underlying topic so that you can have control over its scalability. When you
declare a schema, you choose which columns are part of the key and which are
part of the value. Beyond this, you don't need to think about individual partitions
or offsets. Here are some examples of that.

When a record is processed, its key content is hashed so that its new downstream
partition will be consistent with all other records with the same key. When records are
appended, they follow the correct offset order, even in the presence of
failures or faults. When a stream's key content changes because of how a query
wants to process the rows (via `GROUP BY` or `PARTITION BY`), the underlying
records keys are recalculated, and the records are sent to a new partition in
the new topic set to perform the computation.

## Producers and consumers

Producers and consumers facilitate the movement of records to and from topics.
When an application wants to either publish records or subscribe to them, it
invokes the APIs (generally called the _client_) to do so. Clients communicate
with the brokers (see below) over a structured network protocol.

When consumers read records from a topic, they never delete them or mutate
them in any way. This pattern of being able to repeatedly read the same
information is helpful for building multiple applications over the same data
set in a non-conflicting way. It's also the primary building block for
supporting "replay", where an application can rewind its event stream and read
old information again.

Producers and consumers expose a fairly low-level API. You need to construct
your own records, manage their schemas, configure their serialization, and
handle what you send where.

ksqlDB behaves as a high-level, continuous producer and consumer. You simply
declare the shape of your records, then issue high-level SQL commands that
describe how to populate, alter, and query the data. These SQL programs are
translated into low-level client API invocations that take care of the details
for you.

## Brokers

The brokers are servers that store and manage access to topics. Multiple brokers
can cluster together to replicate topics in a highly-available, fault-tolerant
manner. Clients communicate with the brokers to read and write records.

When you run a ksqlDB server or cluster, each of its nodes communicates with
the {{ site.ak }} brokers to do its processing. From the {{ site.ak }} brokers'
point of view, each ksqlDB server is like a client. No processing takes place
on the broker. ksqlDB's servers do all of their computation on their own nodes.

## Serializers

Because no data format is a perfect fit for all problems, {{ site.ak }} was
designed to be agnostic to the data contents in the key and value portions of
its records. When records move from client to broker, the user payload (key and
value) must be transformed to byte arrays. This enables {{ site.ak }} to work
with an opaque series of bytes without needing to know anything about what they
are. When records are delivered to a consumer, those byte arrays need to be
transformed back into their original topics to be meaningful to the application.
The processes that convert to and from byte representations are called
_serialization_ and _deserialization_, respectively.

When a producer sends a record to a topic, it must decide which serializers to
use to convert the key and value to byte arrays. The key and value
serializers are chosen independently. When a consumer receives a record, it
must decide which deserializer to use to convert the byte arrays back to
their original values. Serializers and deserializers come in pairs. If you use
a different deserializer, you won't be able to make sense of the byte contents.

ksqlDB raises the abstraction of serialization substantially. Instead of
configuring serializers manually, you declare formats using configuration
options at stream/table creation time. Instead of having to keep track of which
topics are serialized which way, ksqlDB maintains metadata about the byte
representations of each stream and table. Consumers are configured automatically
to use the correct deserializers.

## Schemas

Although the records serialized to {{ site.ak }} are opaque bytes, they must have
some rules about their structure to make it possible to process them. One aspect of this
structure is the schema of the data, which defines its shape and fields. Is it
an integer? Is it a map with keys `foo`, `bar`, and `baz`? Something else?

Without any mechanism for enforcement, schemas are implicit. A consumer,
somehow, needs to know the form of the produced data. Frequently this happens
by getting a group of people to agree  verbally on the schema. This approach,
however, is error prone. It's often better if the schema can be managed
centrally, audited, and enforced programmatically.

[Confluent {{ site.sr }}](https://docs.confluent.io/current/schema-registry/index.html), a project outside of {{ site.ak }}, helps with schema
management. {{ site.sr }} enables producers to register a topic with a schema
so that when any further data is produced, it is rejected if it doesn't
conform to the schema. Consumers can consult {{ site.sr }} to find the schema
for topics they don't know about.

Rather than having you glue together producers, consumers, and schema
configuration, ksqlDB integrates transparently with {{ site.sr }}. By enabling
a configuration option so that the two systems can talk to each other, ksqlDB
stores all stream and table schemas in {{ site.sr }}. These schemas can then be
downloaded and used by any application working with ksqlDB data. Moreover,
ksqlDB can infer the schemas of existing topics automatically, so that you 
don't need to declare their structure when you define the stream or table over
it.

## Consumer groups

When a consumer program boots up, it registers itself into a _consumer group_,
which multiple consumers can enter. Each time a record is eligible to be
consumed, exactly one consumer in the group reads it. This effectively provides
a way for a set of processes to coordinate and load balance the consumption of
records.

Because the records in a single topic are meant to be consumed by one process in the group, each
partition in the subscription is read by only one consumer at a time. The number
of partitions that each consumer is responsible for is defined by the total
number of source partitions divided by the number of consumers. If a consumer
dynamically joins the group, the ownership is recomputed and the partitions
reassigned. If a consumer leaves the group, the same computation takes place.

ksqlDB builds on this powerful load balancing primitive. When you deploy a
persistent query to a cluster of ksqlDB servers, the workload is distributed
across the cluster according to the number of source partitions. You don't need
to manage group membership explicitly, because all of this happens automatically.

For example, if you deploy a persistent query with ten source partitions to a
ksqlDB cluster with two nodes, each node processes five partitions. If you lose
a server, the sole remaining server will rebalance automatically and process
all ten. If you now add four more servers, each rebalances to process two partitions.

## Retention and compaction

It is often desirable to clean up older records after some period of time.
Retention and compaction are two different options for doing this. They are both
optional and can be used in conjunction.

Retention defines how long a record is stored before it's deleted. Retention is one of the
only ways to delete a record in a topic. This parameter is
particularly important in stream processing because it defines the time
horizon that you can replay a stream of events. Replay is useful if you're
fixing a bug, building a new application, or backtesting some existing piece of
logic.

ksqlDB enables you to control the retention of the underlying topics of base
streams and tables directly, so it's important to understand the concept. For
more information see [Topics and Logs in the Kafka docs](https://kafka.apache.org/documentation/#intro_topics).

Compaction, by contrast, is a process that runs in the background on each {{ site.ak }}
broker that periodically deletes all but the latest record per key. It is an
optional, opt-in process. Compaction is particularly useful when your records
represent some kind of updates to a piece of a state, and the latest update is
the only one that matters in the end.

ksqlDB directly leverages compaction to support the underlying changelogs that
back its materialized tables. They allow ksqlDB to store the minimum amount of
information needed to rebuild a table in the event of a failover. For more
information see [Log Compaction in the Kafka docs](https://kafka.apache.org/documentation/#compaction).
