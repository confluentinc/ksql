---
layout: page
title: Materialized Views
tagline: Aggregation over streams in ksqlDB
description: Learn about aggregation and materialized views in ksqlDB.  
keywords: ksqldb, query, select, aggregate, materialized view, changelog, redo log, table
---

In any database, one of the main purposes of a table is to enable efficient
queries over the data. ksqlDB stores events immutably in {{ site.aktm }} by
using a simple key/value model. But how can queries be made efficient under
this model? The answer is by leveraging materialized views.

Stream/Table Duality
--------------------

Streams and tables are closely related. A stream is a sequence of events that
you can derive a table from. For example, a sequence of credit scores for a
loan applicant can change over time. The sequence of credit scores is a stream.
But this stream can be interpreted as a table to describe the applicant's
current credit score.

Conversely, the table that represents current credit scores is really two
things: the current credit scores, and also the sequence of changes to the
credit scores for each applicant. This is a profound realization, and much has
been written on this stream/table duality. For more information, see
[Streams and Tables: Two Sides of the Same Coin](https://www.confluent.io/resources/streams-tables-two-sides-same-coin).

Traditional databases have redo logs, but subscribing to changes can be
cumbersome. Redo logs have much shorter retention than the {{ site.ak }}
changelog topic. A fully compacted {{ site.ak }} changelog topic is the same
as a database snapshot. Efficient queries evaluate just the changes.

Materialized Views
------------------

The benefit of a *materialized view* is that it evaluates a query on the
changes only (the delta), instead of evaluating the query on the entire table.

When a new event is integrated, the current state of the view evolves
into a new state. This transition happens by applying the aggregation function
that defines the view with the current state and the new event. When a new
event is integrated, the aggregation function that defines the view is applied
only on this new event, leading to a new state for the view. In this way,
a view is never "fully recomputed" when new events arrive. Instead, the view
adjusts incrementally to account for the new information, which means that
queries against materialized views are highly efficient.

In ksqlDB, a table can be materialized into a view or not. If a table is
created directly on top of a {{ site.ak }} topic, it's not materialized.
Non-materialized tables can't be queried with pull queries (key-based state
lookups), because they would be highly inefficient. On the other hand, if a
table is derived from another collection, ksqlDB materializes its results,
and you can make pull queries against it. This limitation doesn't apply to
push queries (continuous queries).

ksqlDB leverages the idea of stream/table duality by storing both components
of each table. The current state of a table is stored locally and ephemerally
on a specific server by using [RocksDB](https://rocksdb.org/). The series of
changes that are applied to a table is stored durably in a {{ site.ak }} topic
and is replicated across {{ site.ak }} brokers. If a ksqlDB server with a
materialization of a table fails, a new server rematerializes the table from
the {{ site.ak }} changelog.
