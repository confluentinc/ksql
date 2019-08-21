# KLIP 8 - Interactive Queries

**Author**: derekjn | 
**Release Target**: 5.4+ | 
**Status**: In Discussion | 
**Discussion**: [#3117](https://github.com/confluentinc/ksql/pull/3117), [#530](https://github.com/confluentinc/ksql/issues/530)

## Motivation and background

Let’s begin by explicitly defining what is meant by the term “interactive query”:

> An interactive query in KSQL is a SELECT query issued against a KSQL table that runs until completion, returning a finite set of rows as a result. In other words, an interactive query is identical to a SQL query run against a traditional database.

Interactive queries are thus not currently possible in KSQL--all KSQL queries are streaming queries. This can often be surprising to users who have familiarity with other SQL-based systems.

Before exploring what the addition of interactive queries could look like in KSQL, it is important to first understand the existing ways in which users are able to consume query results from KSQL, and why these access patterns may not be sufficient for many use cases. There are currently two ways in which KSQL users may consume the results of their continuous queries, and interactive queries would introduce a third:

## Consuming output topics

Consuming the output topic of a continuous KSQL query is the most straightforward way for users to get query results out of the system. It is the approach that KSQL is currently designed around, and is quite elegant in its natural reliance on Kafka. Topic-based results work very well for any applications that primarily need to understand what is happening by having continuous results pushed to them. This class of application probably represents a relatively narrow (albeit high value) band of use cases. Realtime monitoring, alerting and anomaly detection are good examples of push-based applications.

However, there is an enormous class of applications for which the push-based access pattern is an awkward fit, or even unusable altogether. For example, a dashboard with a date range selector may issue database queries of the form: “give me all the rows from January to April”. While KSQL is very well equipped to generate materialized views that could power such a dashboard, it is not currently possible to efficiently query its underlying materialized state in this way.

The fact that this materialized state already exists within KSQL is a compelling indication that we should unlock the value it holds by allowing users to query it interactively.

## Integrating output topics with an external, queryable datastore

One answer to the restrictions around topic-based results is to implement some kind of integration that sends output topic data to an external datastore that supports interactive querying. In the previous dashboard example, the application would simply issue its queries to this external datastore.

Kafka/KSQL workloads at the edges will always include this kind of custom integration in order to fully serve the workload’s demands. However, if it is required that all users--regardless of workload complexity or scale--must solve the interactive querying problem with external integration, then it is worth considering adding the capability natively to KSQL itself. Given that materialized state is already stored within KSQL, allowing users to interactively query it is a natural progression of KSQL's capabilities.

Furthermore, external datastore integration is not necessarily a trivial undertaking. While it is conceptually straightforward, such an integration becomes a critical infrastructural junction responsible for reliably synchronizing two (or more) persistent stores, probably under a heavy write load. In many ways, data synchronization is the crux of what makes persistent distributed systems so challenging.

For example, if a user is sending KSQL output topic data to PostgreSQL and the external write fails in some way that is not handled or even detected, then PostgreSQL is now potentially permanently out of sync with KSQL. The surface area for these kinds of synchronization challenges is quite large.

---

It is difficult to imagine a comprehensive event-streaming workload that does not include some sort of reasonably generic, queryable datastore. KSQL can naturally evolve into exactly that, giving users the ability to materialize rows as defined by continuous SQL queries and ultimately query them just like they would using a traditional database. In this sense, KSQL would essentially become a streaming database whose transaction log is Kafka.

The remainder of this one pager will propose a high-level approach for incrementally evolving KSQL from a stream-processing system towards a streaming database, releasing the highest ROI capabilities for users first.

## Goals

* Make KSQL fundamentally more useful to application developers by exposing the data already stored within it to conventional `SELECT` queries.
* Simplify the infrastructural requirements for interactive queries against materialized rows generated by KSQL.

## Scope

* **In**
  * The high-level functional capabilities comprising interactive queries will be described.
  * This high-level functionality will be roughly broken down into progressive tiers of completeness.
  * The intended user experience of interactive queries will be described.
* **Out**
  * No specific engineering solutions to any of the capabilities discussed in this one pager will be proposed.
  * Performance challenges of interactive queries will not be discussed.
  * No proposals will be made regarding transactionality/consistency semantics around interactive queries.

## Approach

There is a wide spectrum of complexity between minimally useful row lookups and arbitrarily complex SQL queries over materialized state. Adding support for even the most trivial queries will still add significant value for KSQL users, so we should consider interactive queries an ongoing, incremental effort spanning numerous KSQL releases.

Initial limitations around the kinds of interactive queries KSQL can support will also be augmented by the flexibility that users still have when defining materialized views with continuous queries. For example, even if aggregates are not initially supported for interactive queries, they can still be used in a materialized view’s definition. The same is true for joins.

Here we roughly subdivide the spectrum of interactive query complexity into five progressive tiers, each tier building on the one before it. Each of these tiers can be released separately, although it may make sense to combine some of them.

*Please note that some of the example queries below use mock functions for illustrative purposes.*

### Tier 0 - Simple key-based lookups

Support for key-based lookups is the bare minimum starting point for interactive queries that will still add meaningful utility. This essentially amounts to a cosmetic SQL layer above RocksDB’s key-value interface. Key-based lookups will allow users to look up a single row, given a row key:

* WHERE clause must be strictly of the form: `WHERE ROWKEY = <rowkey>`
* Each query can therefore only target a single shard.
* Arbitrary expressions should be allowed.
* Examples:
  * `SELECT x, y, z FROM table WHERE ROWKEY = ‘rowkey’`
  * `SELECT x + y FROM table WHERE ROWKEY = ‘rowkey’`

### Tier 1 - Key-based range queries

Key-based range queries are a minimal incremental step forward from single-shard queries towards multi-shard queries. Range queries may target multiple shards, but restricting them to be key based will allow us to leverage RocksDB’s native range scan support instead of doing extraneous row scanning:

* Queries with a predicate of the form WHERE ROWKEY > lower_bound AND ROWKEY < upper_bound should be allowed.
* While not technically a range query, queries with a WHERE ROWKEY IN … predicate should be allowed. These are multi-key lookup queries.
* Examples:
  * `SELECT … FROM table WHERE ROWKEY >= 0 AND ROWKEY < 10000`
  * `SELECT … FROM table WHERE ROWKEY < 1000`
  * `SELECT … FROM table WHERE ROWKEY IN (‘k0’, ‘k1’, ‘k2’)`

### Tier 2 - Generic multi-shard queries

While range queries may target multiple shards, expanding to generic multi-shard queries will add significant flexibility to the supported interactive query forms. Multi-shard queries should be able to target any selection of RocksDB rows via comparisons performed aginst any columns:

* Arbitrary WHERE clauses should be allowed.
* Examples:
  * `SELECT … FROM table WHERE x = ‘x’ AND y = ‘y’`
  * `SELECT … FROM table WHERE str LIKE ‘%substr%’`
  * `SELECT … FROM table WHERE x < y`

### Tier 3 - Aggregates

Aggregations are central to extracting value from datasets.

While materialized views in KSQL may already be defined using aggregates, it will still be very useful to perform further aggregations over them via interactive queries. For example, users may want to maintain a materialized view summarized at the minute level but then summarize it further down to the hour level to power a frontend visualization that dynamically supplies a date range:

* All aggregates supported by continuous queries should be supported by interactive queries.
* Examples:
  * `SELECT count(*) FROM table WHERE ROWKEY < 10`
  * `SELECT day(hour), sum(count) FROM table GROUP BY day`
  * `SELECT sum(count) FROM table WHERE ts >= ‘2019-01-01’ AND ts < ‘2019-01-02’`

### Tier 4 - Joins

Joins are also fundamental to deeply analyzing datasets and the relationships between various entities. They will also likely be the most complex to support in a clustered environment, which is why they’ve been put in a separate and final tier. As with aggregates, users may already use joins in their materialized view definitions, which may partially mitigate the cost of their absence within the context of interactive queries.

For these reasons, we should probably not focus much on joins until the previous tiers have been delivered.

* Without relatively robust indexing, it will probably only be initially feasible to join on row keys.
* Examples:
  * `SELECT … FROM t0 JOIN t1 ON t0.ROWKEY = t1.ROWKEY …`
  * `SELECT stats_1m.count AS last_minute, stats_60m.count AS last_hour FROM stats_1m JOIN stats_60m ON stats_1m.ROWKEY = stats_1h.ROWKEY`

## User experience

The user experience for interactive queries should be quite simple: if a user issues a query against a KSQL table, that query should return a finite set of rows as quickly as possible.

Additionally:

* All interactive queries must be issued via the REST API.
* Any interactive query can target any KSQL server.
* Key movement should be handled transparently: if a query launches and a targeted key physically moves, KSQL should redirect the query to the appropriate KSQL server to obtain the correct result.
* For interactive queries against windowed queries, the user may specify a time range via the `WINDOWSTART` and `WINDOWEND` psuedocolumns. For example, `SELECT * FROM windowed_aggregate WHERE ROWKEY = 'rowkey' AND WINDOWSTART >= start_ts AND WINDOWEND < end_ts`

---

# Syntax

All KSQL query syntax currently yields streaming results because KSQL only supports streaming queries. As a result, adding support for point-in-time queries necessitates a means to unambiguously differentiate between streaming and finite query results. The most straightforward approach to this is to use syntax to indicate streaming versus PIT queries. And since there are only two fundamental query forms, it is only necessary to add KSQL syntax to represent one of them.

We therefore propose making minimal syntax changes to represent *streaming* queries. The principal reason for using new syntax for streaming queries is that PIT queries can then use syntax that is as close to the SQL standard as possible, making KSQL interoperable with tooling that integrates with SQL-based systems (i.e. dashboarding frontends).

After introducing any syntax changes, KSQL's syntax must make sense in each of the following contexts:

* Streaming query on a stream
* Streaming query on a table
* Point-in-time query on a stream
* Point-in-time query on a table
* Transient queries defined by any of the above
* CTAS/CSAS defined by any of the above

*Note that not all of these query forms need to be supported (initially, or ever), but a sensible design should allow for all of them with minimal ambiguity.*

The proposed syntax changes will now be specifically described.

## Proposed syntax changes

Streaming queries may be identified via the `EMIT CHANGES` query modifier:

```sql
SELECT ... FROM stream EMIT CHANGES;
```

Partially inspired by `EMIT STREAM` from this excellent [SIGMOD paper](https://arxiv.org/pdf/1905.12133.pdf), `EMIT CHANGES` has been proposed in favor of `EMIT STREAM` for the following reasons:

* De-emphasizes the stream abstraction as we continue to consider making KSQL more table centric.
* Not redundant in the context of CSAS (`CREATE STREAM s0 AS SELECT .. FROM s1 EMIT STREAM`).
* Intuitive and descriptive in the context tables. A streaming query against a table effectively produces rows representing changes to the underlying table. Identifying this changelog as simply a stream isn't necessarily clear.
* Similarly to the above, `EMIT CHANGES` is intuitive in the context of streaming aggregations, which produce incremental *changes* over time.
* `EMIT STREAM` generally feels better suited for transient queries, for which there is no other `STREAM` context in the query (i.e. CSAS).

The following are canonical example queries within each of the aforementioned fundamental query contexts:

```sql
-- Streaming query on a stream
SELECT x, count(*) FROM stream GROUP BY x EMIT CHANGES;

-- Streaming query on a table
SELECT x, y FROM table WHERE x < 10 EMIT CHANGES;

-- Point-in-time query on a table
SELECT ROWKEY, count FROM table WHERE ROWKEY = 'key';

-- Point-in-time query on a stream (semantics unclear, illustrative example only)
SELECT * FROM stream WHERE column = 42;
```

---

Finally, `EMIT CHANGES` may be preferrable over `EMIT STREAM` for KQSL specifically (for the reasons outlined above) although `EMIT STREAM` is still a suitable option.

## Proposed behavioral changes

As we add this fundamentally new capability to KSQL, it is worth considering any modifications we can make to KSQL's existing behavior to ensure query behavior is as intuitive as possible after the addition of PIT queries. There is one potential behavioral change that is worth considering in this respect: **streaming queries against a table should return the table's entire materialized state (possibly filtered by a `WHERE` clause) followed by changes.**

Currently, a streaming query against a table in KSQL will only return rows from the table's changelog produced *after* the query begins (assuming a default start offset of `latest`).

However, streaming queries returning a table's entire materialized state *then* followed by changes is more consistent with the behavior of PIT queries on a table. This behavior is also likely what users want when issuing a streaming query against a table, and it is currently not straightforward to achieve.

To get KSQL's current behavior with a streaming query against a table, users would simply use a simple, intuitive `WHERE` clause:

```sql
SELECT * FROM table WHERE ROWTIME >= NOW
```

## Limitations

The following are proposed intentional limitations of the initial implementation of PIT queries:

**PIT queries on streams probably do not need to be initially supported.** The semantics and use cases around these are unclear and require further analysis. However, the proposed syntax leaves open the possibility of supporting PIT queries on streams without further modifications.

**CTAS/CSAS defined by PIT queries do not initially need to be supported.** For example, the following CTAS would create a table, `t0`, and initially populate it with the result of the `SELECT` query:

```sql
CREATE TABLE t0 AS SELECT x, count(*) FROM t1 GROUP BY x;
```

**PIT queries against tables created directly over a topic (leaf tables) do not need to be initially supported.** Leaf tables are not materialized until another query reads from them. As a result, a PIT query against such a table would (potentially) not return the expected result. Rather than leave open the possibility of confusing query results in this scenario, we should simply disallow PIT queries against leaf tables until we decide to support them.

