---
layout: page
title: Evolving Production Queries
tagline: Replacing existing queries
description: Learn how to manage your production deployments over time
keywords: ksqldb, upgrade, schema evolution
---

Production deployments of databases are never static; they evolve as application
and business requirements change. To that end, all popular data stores have ways
of managing and manipulating existing data. For stream processing applications,
you may want to modify your application because of:

- Business Requirements: requirements simply change over time
- Schema Evolution: the incoming data or required output has been modified
- Optimizations: the same application can be executed more efficiently (either
  by user or engine)

ksqlDB provides various mechanisms to interact with a query that's running in
production.

1. *In-place upgrades*: users modify the behavior of a query, resuming from a
   previously committed offset. The syntax that ksqlDB uses to indicate an
   in-place upgrade is `CREATE OR REPLACE`.
1. *Replacing upgrades*: these upgrades require you to tear down existing
   queries, and start a new one from either `earliest` or `latest` offsets.
   To accomplish this, users you first issue a `TERMINATE <query_id>;` and a
   `DROP <source>` before creating the query again.

## Understanding upgrades

To better understand the different types of upgrades that exist on continuous
queries, we define a taxonomy on query upgrades as any combination of three
types of characteristics: _source query_, _upgrade_ and (optionally) _environment_.

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

ksqlDB supports only in-place upgrades for _data selection_ and
_schema evolution_ upgrades on a limited subset of query characteristics.
ksqlDB doesn't guarantee validity of any environments when performing an
in-place upgrade.

Any in place upgrades on windowed or joined sources, as well as upgrades on
any table aggregation, are not yet supported.

## A motivating example

Imagine a query that reads from a stream of purchases made at ksqlDB's flagship
store, ksqlMart, and filters out transactions that might be invalid:

```sql
CREATE STREAM purchases (product_id INT KEY, name VARCHAR, cost DOUBLE, quantity INT);
CREATE STREAM valid_purchases AS SELECT * FROM purchases WHERE cost > 0.00 AND quantity > 0;
```

### Data selection (simple)

Over time, ksqlMart changes its return policy and begins issuing full refunds.
These events have a negative `cost` column value. Since these events are now
valid, ksqlMart needs to update the query to remove the `cost > 0.00` clause:

```sql
CREATE OR REPLACE STREAM valid_purchases AS SELECT * FROM purchases WHERE quantity > 0;
```

This `CREATE OR REPLACE` statement instructs ksqlDB to terminate the old query,
and create a new one with the new semantics that will continue from the last
event that the previous query processed. Note that this means any previously 
processed data with negative cost will not be included, even if issuing the
query with `SET 'auto.offset.reset'='earliest';`.

This query upgrade is a _simple_, _data selecting_ upgrade because it doesn't
involve any aggregations; the only change is the criteria to emit rows. ksqlDB
supports nearly all _data selection_ modifications on source queries.

### Schema evolution

Over time, ksqlMart gets more sophisticated in their usage of {{ site.ak }} to
monitor their input. They start publishing a new field to the `purchases` stream,
named `popularity`. In order to reflect this change in their `valid_purchases`
stream, they need to issue two different commands:

```sql
CREATE OR REPLACE STREAM purchases (product_id INT KEY, name VARCHAR, cost DOUBLE, quantity INT, popularity DOUBLE); 
CREATE OR REPLACE STREAM valid_purchases AS SELECT * FROM purchases WHERE quantity > 0;
```

There are a few things to note in the above statements:

1. DDL statements can be updated using `CREATE OR REPLACE`.
2. ksqlMart re-issued the `SELECT *` statement even though the statement text is
   identical to the previous statement they issued.
3. Why is (2) necessary? ksqlDB resolves `SELECT *` at the time the query was
   issued, which means that any updates to `purchases` after issuing a
   `CREATE AS SELECT` statement aren't picked up in `valid_purchases`.

_Schema Evolution_ upgrades have much stricter requirements than _Data Selection_
upgrades. ksqlDB supports only adding new fields. Removing, renaming, or changing
the type of any field is invalid.

### Stateful data selection

The previous examples all involve _stateless_ upgrades, but ksqlDB also enables
_data selection_ and limitted _schema evolution_ upgrades on some stateful queries. 
ksqlMart, as is common with data-driven companies that leverage ksqlDB, also has 
queries that generate analytics on their purchases:

```sql
CREATE TABLE purchase_stats
    AS SELECT product_id, COUNT(*) AS num_sales, AVG(cost * quantity) AS average_sale
    FROM valid_purchases 
    GROUP BY product_id;
```

After some time, they realize that the `purchase_stats` stream doesn't account
properly for refunds. They're OK with having the initial purchase count toward
the `purhcase_stats`, but they don't want the refund to increment the `COUNT(*)`
aggregation, so they update their query in place to add a filter for this
condition:

```sql
CREATE OR REPLACE TABLE purchase_stats
    AS SELECT product_id, COUNT(*) AS num_sales, AVG(cost * quantity) AS average_sale
    FROM valid_purchases 
    WHERE cost > 0
    GROUP BY product_id;
```

This updated query ensures only that _new_ refunds don't count toward the stats,
but anything that was counted before will remain.

If ksqlMart wanted to _backfill_ the data properly, they would need to issue a 
replacing upgrade that read from the earliest offset in the `valid_purchases` stream:

```sql
TERMINATE CTAS_PURCHASE_STATS_0;
DROP STREAM purhcase_stats;
SET 'auto.offset.reset'='earliest';

-- read from the start and create a new table
CREATE TABLE purchase_stats
    AS SELECT product_id, COUNT(*) AS num_sales, AVG(cost * quantity) AS average_sale
    FROM valid_purchases 
    WHERE cost > 0
    GROUP BY product_id;
```

This solution becomes more difficult if there are downstream consumers of the
`purchase_stats` table.

### Additional restrictions on stateful data selection

ksqlDB maintains state in order to accomplish stateful tasks such as
aggregations. To ensure that all intermediate state is compatible, ksqlDB
ensures that the intermediate schema is identical when changing a filter, which
means that ksqlMart can only change filters to include fields that are already
selected and can't remove a filter which is the only reference to a field.
