---
layout: page
title: Evolving Production Queries
tagline: Replacing existing queries
description: Learn how to manage your production deployments over time
keywords: ksqldb, upgrade, schema evolution
---

Production deployments of databases are never static; they evolve as application and business 
requirements change. To that end, all popular data stores have ways of managing and manipulating 
existing data. For stream processing applications, a user may want to modify their application as 
a result of:

- Business Requirements: requirements simply change over time
- Schema Evolution: the incoming data or required output has been modified
- Optimizations: the same application can be executed more efficiently (either by user or engine)

ksqlDB provides various mechanisms to interact with a query that is running in production, but they
boil down into two categories:

1. *In Place Upgrades*: these upgrades allow users to modify the behavior of a query, resuming from
    a previously committed offset. The syntax that ksqlDB uses to indicate an in place upgrade is
    `CREATE OR REPLACE`.
1. *Replacing Upgrades*: these upgrades require users to tear down existing queries, and start a new
    one from either `earliest` or `latest` offsets. To accomplish this, users must first issue a
    `TERMINATE <query_id>;` and a `DROP <source>` before creating the query again.

Understanding Upgrades
----------------------

To better understand the different types of upgrades that exist on continuous queries, we define a 
taxonomy on query upgrades as any combination of three types of characteristics: **source query, 
upgrade** and (optionally) **environment**:

| **Category** | **Characteristic** | **Description** |
|----------|----------------|-------------|
| Query | Stateful | Stateful queries maintain local storage |
| | Windowed | Windowed queries maintain a limited amount of state specified by a window in time
| | Joined | Joined queries read from multiple sources
| | Multistage | Multistage queries contain intermediate, non-user visible topics in Kafka
| | Nondeterministic | Nondeterministic queries may produce different results when executing identical input
| | Simple | Queries with none of the above characteristics
| Upgrade | Transparent | Transparent upgrades change the way something is computed (e.g. improving a UDF performance)
| | Data Selection | Data selecting query upgrades change which/how many events are emitted
| | Schema Evolution | Schema evolving query upgrades change the output type of the data |
| | Source Modifying | These upgrades change the source data, whether by means of modifying a JOIN or swapping out a source |
| | Topology | These upgrades are invisible to the user, but change the topology, such as the number of sub-topologies or the ordering of operations (e.g. filter push down) |
| | Scaling | Scaling upgrades change the physical properties of the query in order to enable better performance characteristics. |
| | Unsupported | Unsupported upgrades are ones that will semantically change the query in an unsupported way. There are no plans to implement these migrations. |
| Environment | Backfill | Backfill requires the output data to be accurate not just from a point in time, but from the earliest point of retained history |
| | Cascading | Cascading environments contain queries that are not terminal, but rather feed into downstream stream processing tasks |
| | Exactly Once | Exactly Once environments do not allow for data duplication or missed events |
| | Ordered | Ordered environments require that a single offset delineates pre- and post-migration (no events are interleaved) |
| | Live | Live environments describe queries that cannot afford downtime, either by means of acting as live storage (e.g. responding to pull queries) or feeding into high availability systems (powering important functionality) |

Different use upgrades will fall into this classification. Take the scenarios below as examples:

### Data Selection (Simple)

Imagine you have a query which reads from a stream of purchases made at your store, ksqlMart and
filters out transactions that might be invalid:

```sql
CREATE STREAM purchases (product_id INT KEY, name VARCHAR, cost DOUBLE, quantity INT);
CREATE STREAM valid_purchases AS SELECT * FROM purchases WHERE cost > 0.00 AND quantity > 0;
```

Over time, ksqlMart changes its return policy and begins issuing full refunds. These events have
a negative `cost` column value. Since these events are now valid, ksqlMart needs to update the 
query (removing the `cost > 0.00` clause):

```sql
CREATE OR REAPLCE valid_purchases AS SELECT * FROM purchases WHERE quantity > 0;
```

This `CREATE OR REPLACE` statement instructs ksqlDB to terminate the old query, and create
a new one with the new semantics that will continue from the last event that the previous 
query processed.

This query upgrade is a _simple_, _data selecting_ upgrade because it does not involve any
aggregations; the only change is the criteria to emit rows. ksqlDB supports nearly all _data
selection_ modifications on source queries.

### Schema Evolution

Over time, ksqlMart gets more sophisticated in their usage of Kafka to monitor their input. They
start publishing a new field to the `purchases` stream: `popularity`. In order to reflect that 
in their `valid_purchases` stream, they need to issue two different commands:

```sql
CREATE OR REPLACE STREAM purchases (product_id INT KEY, name VARCHAR, cost DOUBLE, quantity INT, popularity DOUBLE); 
CREATE OR REPLACE STREAM valid_purchases AS SELECT * FROM purchases WHERE quantity > 0;
```


