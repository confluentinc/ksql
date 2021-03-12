---
layout: page
title: ksqlDB Performance Guidelines
tagline:  in ksqlDB
description:  ksqlDB. 
keywords: performance
---

One of the principal benefits of using ksqlDB for stream processing is that it
enables you to define your workload declaratively. All ksqlDB stream processing
logic is defined using SQL statements, which are internally transformed into
execution plans that perform the necessary computations as efficiently as
possible. While abstracting away internal execution plans underneath a SQL
layer enables rapid development of stream processing workloads, it also
requires having some level of knowledge about internal system behavior for cases
in which optimal performance is desired.

This document provides an overview of key factors affecting ksqlDB performance,
so you can develop performant stream processing workloads using ksqlDB. A basic
understanding of ksqlDB’s key abstractions is assumed, so if you're new to
ksqlDB we recommend reading through the [core concepts](index.md) documentation
before proceeding.

These guidelines are simply recommendations around how to get the most
performance out of your ksqlDB workload. For example, if your workload is
unable to keep up with the rate at which your input data is being produced,
these guidelines may help to increase your workload's throughput to keep it
from falling behind.

## Persistent queries

Typically, a ksqlDB workload is composed of one or more persistent queries.
These queries consume input continuously from {{ site.aktm }} topics and produce
their output back into {{ site.ak }} topics. Although this document describes
how to define these persistent queries such that they are as efficient as
possible, the number of persistent queries running on a ksqlDB deployment has
system-wide performance implications, regardless of how simple or complex they
may be.

### Combine persistent queries

Combine as much work as possible into as few persistent queries as possible.
Each individual persistent query within a ksqlDB workload incurs a significant
amount of system resource overhead. As a result, you should structure your
workload such that all required work is consolidated into as few persistent
queries as possible. A workload consolidated into a single persistent query
requires significantly less system-resource overhead as the same workload
spread across two persistent queries.

Minimizing the number of persistent queries that your workload uses is probably
the single most impactful design pattern that you can leverage, so it's a
recurring theme throughout this performance guide. You'll learn more about the
specific benefits of running fewer persistent queries in the following sections.

## State stores

ksqlDB workloads consume their input from {{ site.ak }} topics and
produce their output into {{ site.ak }} topics, but some local state is also
maintained on each ksqlDB node. This state effectively serves as a local cache
of topic data that improves workload performance. Local ksqlDB state is managed
by using a pluggable key-value storage interface. RocksDB is the default storage
engine for local state.

These state stores are used in a number of contexts, which are covered in the following sections, but we’re highlighting them first to contextualize the remainder of this document, as your workload’s performance will benefit from following some general guidelines around state stores.

Furthermore, it is important to note that in addition to being stored as local key-value stores, each ksqlDB state store is also backed by a changelog Kafka topic in Kafka. Storing a complete log of all changes made to each local state store allows ksqlDB to recover in the event of a node failure.

### Minimize the size of your state stores

State store sizes are proportional to the size of each key-value record that
they contain. Smaller state stores reduce the amount of disk read-and-write
throughput performed by ksqlDB, resulting in better overall workload throughput.
Also, smaller state stores minimize local and {{ site.ak }} disk storage
consumption. State store row sizes may be minimized by eliminating unused
fields in your events.

Minimize the number of state stores that your ksqlDB deployment uses. ksqlDB batches writes made to local state stores, so fewer state stores allows for larger batches of writes to a given state store, which in turn improves disk write throughput. Similarly to the above, fewer state stores will minimize both local and Kafka disk storage requirements.

As we’ve alluded to above, you will learn how to influence the number and size of ksqlDB’s state stores in the sections to follow.

## Serialization and deserialization

Input and output events consumed and produced by ksqlDB workloads are
serialized as {{ site.ak }} messages, so each consumed event must be
deserialized before being processed by ksqlDB. Similarly, each output event
produced by ksqlDB must be serialized before being written to {{ site.ak }}.
As a result, event serialization is generally the most expensive aspect of
any ksqlDB workload.

While we would not recommend always choosing whichever serialization format is most performant for your data model (you should use whichever format best suits your use case), we do recommend that you remain mindful of the fact that event serialization is usually the dominant cost for most workloads.

### Avoid unnecessary serialization/deserialization operations

Avoid extraneous serialization and deserialization operations. Combine as much
work as possible into as few persistent queries as your use case allows. For
example, instead of performing a transformation with one persistent query and
filtering its output with another persistent query, consider combining the
transformation and filter into a single persistent query. In this case,
serialization and deserialization overhead would be reduced approximately by
half.

### Reduce event complexity and size

Avoid unnecessary event complexity and size. Smaller, simpler events are more
efficient to serialize and deserialize. If your workload doesn’t require
certain fields in your input messages, consider removing these fields as early
as possible to improve serialization performance. In some cases, it may be
preferable to use ksqlDB itself to perform this message simplification by
selecting a subset of the messages’ fields to prepare them for downstream
processing.

## Transformations and filters

A transformation is any reference to a column that is not a bare column
reference. For example, a function call over a column, and an arithmetic
expression involving one or more columns are both transformations. A filter
is a Boolean expression invoked via a WHERE clause that excludes any events
for which that expression evaluates to false. Transformations and filters
are thus evaluated for each input event, and typically incur a negligible
amount of performance overhead. This does not necessarily apply to user-defined
function invocations.

### Consolidate transformations and filters into a few persistent queries

Consolidate your transformations and filters into as few persistent queries as
possible. Although transformations and filters generally aren't performance
bottlenecks, each individual persistent query does incur a significant amount
of system-resource overhead. For this reason, you should combine as many of
your transformations and filters as possible into the fewest number of
persistent queries that your workload allows. Additionally, for any
transformations that reduce event size, we recommend that you perform these
transformations as early as possible in your processing pipeline.

## Topic repartitioning

ksqlDB relies heavily on topic partitioning to spread work across a deployment.
For aggregations and joins, it's especially important to understand partitioning
semantics.

For aggregations, ksqlDB must ensure that each node in a deployment
is always guaranteed to process the same subset of aggregation groupings, which
enables each ksqlDB node to accumulate aggregate results independently of other
nodes in the deployment.

For joins, both sides of the join must be partitioned using the same key, known
as co-partitioning, to guarantee that events for both sides of the join are
always processed by the same ksqlDB node.

ksqlDB re-partitions input topics  automatically if they aren't partitioned in a
way that satisfies these partitioning requirements for aggregations and joins.

Whenever ksqlDB repartitions a topic, it creates an internal topic to store the
repartitioned data. These internal topics essentially duplicate their input
topics using the required partitioning key.

Avoiding this data duplication can reduce the amount of topic storage consumed
by your {{ site.ak }} cluster. Also, it can reduce the volume of network round
trips between ksqlDB and {{ site.ak }}.

### Key your streams and tables to fit your workload

Key your streams and tables explicitly to fit your workload. You may specify
partitioning keys by using the KEY column qualifier or a PARTITION BY clause
for persistent queries.

### Key your JOIN inputs on columns used downstream

Whenever possible, key your JOIN input streams and tables on the columns that
will be used in downstream joins. If input streams and tables are co-partitioned
already, no internal repartitioning in required to join them.

### Key your aggregation inputs on the aggregation grouping criteria

Whenever possible, key your aggregation inputs on the aggregation grouping
criteria. For cases in which you are aggregating a stream using a single
grouping column, consider keying the input stream on that column if your
workload permits. This avoids an internal repartitioning. When aggregating
using multiple grouping columns, ksqlDB generates a composite key derived
from each grouping column, so repartitioning is more difficult to avoid.

### Avoid unnecessary repartitioning

If repartitioning is unavoidable for some aspect of your workload, consider
sharing any repartitioned data in order to avoid extraneous repartitions. For
example, if you know that you’ll be aggregating a stream over column ``col1``
and also joining against ``col1`` somewhere else, keying the stream explicitly
on ``col1`` will avoid two repartitions. This is another instance in which
consolidating logic into as few persistent queries as possible can benefit your
workload’s efficiency, by reducing repartitioning overhead.

## Parallelization

To provide scalability and fault tolerance, ksqlDB parallelizes workloads
across nodes in a deployment. This parallelism is achieved by leveraging
topic partitions. For a given persistent query, each node in a ksqlDB
deployment processes a subset of the persistent query’s input partitions.
This is an important consideration to take into account when designing a
workload that leverages all of the available resources within a ksqlDB
deployment.

### Use an adequate number of partitions

Spread your workload across your ksqlDB deployment by using an appropriate
number of partitions. If a persistent query’s input stream has two partitions,
and your ksqlDB deployment has four nodes, two of these nodes aren't performing
any work for the persistent query. For this reason, input streams and tables
should generally have more partitions than there are nodes in a ksqlDB
deployment. 

!!! important

    A given node in a ksqlDB deployment can handle more than one input
    partition, so partition counts don't need to be the same as the node count.

Stateful persistent queries create one state store per partition, so too many
partitions can result in an excessive number of state stores. Ultimately, you
need adequate partitions to parallelize your workload completely across all
nodes in your cluster, but no more than that. Using twice as many partitions
as you have nodes in your cluster is generally a good balance.

## Aggregations

ksqlDB provides one of the easiest ways to compute aggregations over your topic
data stored in {{ site.ak }}. These aggregations are defined as persistent
queries whose results are maintained within a ksqlDB table, optionally with a
time window as a dimension. Since aggregations are inherently stateful, they
rely heavily upon local materialized state stores in order to efficiently
accumulate ongoing aggregation results. These state stores maintain a local
cache of current aggregate results for the subset of persistent query partitions
that are assigned to a given node in a deployment.

### Consolidate aggregation calls into as few persistent queries as possible

Frequently, aggregations aren't done in isolation, for example, a SUM call is 
often accompanied by a COUNT call. Whenever the aggregation grouping criteria, 
and, potentially, filtering criteria, are equivalent for multiple aggregations,
you should combine these aggregate calls into the same persistent query.
Aggregation consolidation can potentially avoid duplicitous repartitioning in
cases for which repartitioning is necessary, as well as minimize the number of
state stores required for a given set of aggregations.

### Avoid unnecessarily narrow WINDOW widths

Aggregations that use a WINDOW clause add dimensionality to the aggregation’s
grouping criteria. Think of the aggregation’s WINDOW as an implicit grouping
column. Each aggregation group requires local materialization, via state stores,
of up to one row per grouping criteria, per window. For example, a window width
of one hour will result in 24 times more windows than a window width of one day.
As a result, the number of locally materialized rows required for a windowed
aggregation is inversely proportional to the window’s width. We recommend using
aggregation windows with the widest width that your use case will allow.

## Joins

ksqlDB provides rich support for joining together multiple streams and tables.
While such JOIN operations are relatively easy to express via SQL, in certain
cases they can become expensive to execute internally.

### Avoid unnecessarily wide JOIN windows

Stream-stream joins require that you specify a window over which to perform the
join. Events on each side of the join match only if they both occur within the
specified window. The amount of local state required for a stream-stream join
is directly proportional to the width of the join window. As a result, we
recommend using stream-stream join window widths that are no wider than your
use case permits.

### Ensure both sides of a JOIN are co-partitioned whenever possible

ksqlDB requires that both sides of a join operation are partitioned identically
in order to guarantee that matching events are always processed by the same
ksqlDB node. Whenever two input streams or tables are not co-partitioned,
ksqlDB must perform an internal repartition, thereby duplicating topic data as
well as network round trips issued by the persistent query performing the join.


