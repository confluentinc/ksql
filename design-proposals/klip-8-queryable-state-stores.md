# KLIP 8 - Queryable State Stores

**Author**: derekjn | 
**Release Target**: 5.4+ | 
**Status**: _Merged_ | 
**Discussion**: [#3117](https://github.com/confluentinc/ksql/pull/3117), [#530](https://github.com/confluentinc/ksql/issues/530)

## Motivation and background

KSQL currently supports one fundamental query form: streaming queries that "push" ongoing query results out to clients that are subscribed to their output or to tables that persist the results. A table persisting the output of a streaming query is thus a materialized view that is perpetually updated by the incremental output of the streaming query it's defined by. And since KSQL currently only supports streaming queries, the state of a materialized view may only be retrieved by consuming its changelog, which is not always an ideal access pattern.

This KLIP proposes adding a fundamentally new query form that is complementary to KSQL's push-based query results: ***pull queries***. In direct contrast to a streaming query that continuously produces output over a *span* of time, a pull query *pulls* data from a table for a specific *point* in time:

> A pull query in KSQL is a SELECT query issued against a KSQL table that runs until completion, returning a finite set of rows as a result. In other words, a pull query is identical to a SQL query run against a traditional database.

Pull queries are thus not currently possible in KSQL--all KSQL queries are streaming queries. This can often be surprising to users who have familiarity with other SQL-based systems.

Before exploring what the addition of pull queries could look like in KSQL, it is important to first understand the existing ways in which users are able to consume query results from KSQL, and why these access patterns may not be sufficient for many use cases. There are currently two ways in which KSQL users may consume the results of their continuous queries, and pull queries would introduce a third:

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
* Simplify the infrastructural requirements for pull queries against materialized rows generated by KSQL.

## Scope

* **In**
  * The high-level functional capabilities comprising pull queries will be described.
  * This high-level functionality will be roughly broken down into progressive tiers of completeness.
  * The intended user experience of pull queries will be described.
* **Out**
  * No specific engineering solutions to any of the capabilities discussed in this one pager will be proposed.
  * Performance challenges of pull queries will not be discussed.
  * No proposals will be made regarding transactionality/consistency semantics around pull queries.

## Approach

There is a wide spectrum of complexity between minimally useful row lookups and arbitrarily complex SQL queries over materialized state. Adding support for even the most trivial queries will still add significant value for KSQL users, so we should consider pull queries an ongoing, incremental effort spanning numerous KSQL releases.

Initial limitations around the kinds of pull queries KSQL can support will also be augmented by the flexibility that users still have when defining materialized views with continuous queries. For example, even if aggregates are not initially supported for pull queries, they can still be used in a materialized view’s definition. The same is true for joins.

Here we roughly subdivide the spectrum of pull query complexity into five progressive tiers, each tier building on the one before it. Each of these tiers can be released separately, although it may make sense to combine some of them.

*Please note that some of the example queries below use mock functions for illustrative purposes.*

### Tier 0 - Simple key-based lookups

Support for key-based lookups is the bare minimum starting point for pull queries that will still add meaningful utility. This essentially amounts to a cosmetic SQL layer above RocksDB’s key-value interface. Key-based lookups will allow users to look up a single row, given a row key:

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

While range queries may target multiple shards, expanding to generic multi-shard queries will add significant flexibility to the supported pull query forms. Multi-shard queries should be able to target any selection of RocksDB rows via comparisons performed against any columns:

* Arbitrary WHERE clauses should be allowed.
* Examples:
  * `SELECT … FROM table WHERE x = ‘x’ AND y = ‘y’`
  * `SELECT … FROM table WHERE str LIKE ‘%substr%’`
  * `SELECT … FROM table WHERE x < y`

### Tier 3 - Aggregates

Aggregations are central to extracting value from datasets.

While materialized views in KSQL may already be defined using aggregates, it will still be very useful to perform further aggregations over them via pull queries. For example, users may want to maintain a materialized view summarized at the minute level but then summarize it further down to the hour level to power a frontend visualization that dynamically supplies a date range:

* All aggregates supported by continuous queries should be supported by pull queries.
* Examples:
  * `SELECT count(*) FROM table WHERE ROWKEY < 10`
  * `SELECT day(hour), sum(count) FROM table GROUP BY day`
  * `SELECT sum(count) FROM table WHERE ts >= ‘2019-01-01’ AND ts < ‘2019-01-02’`

### Tier 4 - Joins

Joins are also fundamental to deeply analyzing datasets and the relationships between various entities. They will also likely be the most complex to support in a clustered environment, which is why they’ve been put in a separate and final tier. As with aggregates, users may already use joins in their materialized view definitions, which may partially mitigate the cost of their absence within the context of pull queries.

For these reasons, we should probably not focus much on joins until the previous tiers have been delivered.

* Without relatively robust indexing, it will probably only be initially feasible to join on row keys.
* Examples:
  * `SELECT … FROM t0 JOIN t1 ON t0.ROWKEY = t1.ROWKEY …`
  * `SELECT stats_1m.count AS last_minute, stats_60m.count AS last_hour FROM stats_1m JOIN stats_60m ON stats_1m.ROWKEY = stats_1h.ROWKEY`

## User experience

The user experience for pull queries should be quite simple: if a user issues a pull against a KSQL table, that query should return a finite set of rows as quickly as possible.

Additionally:

* All pull queries must be issued via the REST API.
* Any pull query can target any KSQL server.
* Key movement should be handled transparently: if a query launches and a targeted key physically moves, KSQL should redirect the query to the appropriate KSQL server to obtain the correct result.
* For pull queries against windowed queries, the user may specify a time range via the `WINDOWSTART` and `WINDOWEND` psuedocolumns. For example, `SELECT * FROM windowed_aggregate WHERE ROWKEY = 'rowkey' AND WINDOWSTART >= start_ts AND WINDOWEND < end_ts`

---

# Syntax changes

All KSQL query syntax currently yields streaming results because KSQL only supports streaming queries. As a result, adding support for pull queries necessitates a means to unambiguously differentiate between streaming and finite query results. The most straightforward approach to this is to use syntax to indicate streaming versus pull queries. And since there are only two fundamental query forms, it is only necessary to add KSQL syntax to represent one of them.

We therefore propose making minimal syntax changes to represent *streaming* queries. The principal reason for using new syntax for streaming queries is that pull queries can then use syntax that is as close to the SQL standard as possible, making KSQL interoperable with tooling that integrates with SQL-based systems (i.e. dashboarding frontends).

After introducing any syntax changes, KSQL's syntax must make sense in each of the following contexts:

* Streaming query on a stream
* Streaming query on a table
* Pull query on a stream
* Pull query on a table
* CTAS/CSAS defined by any of the above

*Note that not all of these query forms need to be supported (initially, or ever), but a sensible design should allow for all of them with minimal ambiguity.*

The proposed syntax changes will now be specifically described.

## Proposed syntax changes

Streaming queries may be identified via the `EMIT CHANGES` query modifier:

```sql
SELECT ... FROM stream EMIT CHANGES;
```

Inspired by `EMIT STREAM` from this excellent [SIGMOD paper](https://arxiv.org/pdf/1905.12133.pdf), as well as @big-andy-coates' [brainstorm](https://github.com/confluentinc/ksql/pull/3117#issuecomment-520524284), `EMIT CHANGES` has been proposed in favor of `EMIT STREAM` for the following reasons:

* De-emphasizes the stream abstraction as we continue to consider making KSQL more table centric.
* Not redundant in the context of CSAS (`CREATE STREAM s0 AS SELECT ... FROM s1 EMIT STREAM`).
* Intuitive and descriptive in the context of tables. A streaming query against a table effectively produces rows representing changes to the underlying table. Identifying this changelog as simply a stream isn't necessarily clear.
* Similarly to the above, `EMIT CHANGES` is intuitive in the context of streaming aggregations, which produce incremental *changes* over time.
* `EMIT STREAM` generally feels better suited for ad-hoc queries, for which there is no other `STREAM` context in the query (i.e. CSAS).

The following are canonical example queries within each of the aforementioned fundamental query contexts:

```sql
-- Streaming (push) query on a stream
SELECT x, count(*) FROM stream GROUP BY x EMIT CHANGES;

-- Streaming (push) query on a table
SELECT x, y FROM table WHERE x < 10 EMIT CHANGES;

-- Point-in-time (pull) query on a table
SELECT ROWKEY, count FROM table WHERE ROWKEY = 'key';

-- Point-in-time (pull) query on a stream (semantics unclear, illustrative example only)
SELECT * FROM stream WHERE column = 42;
```

---

Finally, `EMIT CHANGES` may be preferrable over `EMIT STREAM` for KSQL specifically (for the reasons outlined above) although `EMIT STREAM` is still a suitable option.

## Proposed behavioral changes

As we add this fundamentally new capability to KSQL, it is worth considering any modifications we can make to KSQL's existing behavior to ensure query behavior is as intuitive as possible after the addition of pull queries. There is one potential behavioral change that is worth considering in this respect: **streaming queries against a table should return the table's entire materialized state (possibly filtered by a `WHERE` clause) followed by changes.**

Currently, a streaming query against a table in KSQL will only return rows from the table's changelog produced *after* the query begins (assuming a default start offset of `latest`).

However, streaming queries returning a table's entire materialized state *then* followed by changes is more consistent with the behavior of pull queries on a table. This behavior is also likely what users want when issuing a streaming query against a table, and it is currently not straightforward to achieve.

To get KSQL's current behavior with a streaming query against a table, users would simply use an intuitive `WHERE` clause:

```sql
SELECT * FROM table WHERE ROWTIME >= NOW EMIT CHANGES;
```

## Limitations

The following are proposed intentional limitations of the initial implementation of pull queries:

**Pull queries on streams probably do not need to be initially supported.** The semantics and use cases around these are unclear and require further analysis. However, the proposed syntax leaves open the possibility of supporting pull queries on streams without further modifications.

**CTAS/CSAS defined by pull queries do not initially need to be supported.** For example, the following CTAS would create a table, `t0`, and initially populate it with the result of the `SELECT` query:

```sql
CREATE TABLE t0 AS SELECT x, count(*) FROM t1 GROUP BY x;
```

**Pull queries against tables created directly over a topic (leaf tables) do not need to be initially supported.** Leaf tables are not materialized until another query reads from them. As a result, a pull query against such a table would (potentially) not return the expected result. Rather than leave open the possibility of confusing query results in this scenario, we should simply disallow pull queries against leaf tables until we decide to support them.

## Backward compatibility

The proposed syntax changes for streaming queries is backward incompatible with previous KSQL releases. However, we may mitigate the impact of this backward incompatibility substantially by making the `EMIT CHANGES` clause implicit for all existing *persistent queries*. That is, with the initial release containing support for queryable state stores, the following two persistent query definitions will be equivalent:

```sql
CREATE STREAM s0 AS SELECT * FROM s1;
CREATE STREAM s0 AS SELECT * FROM s1 EMIT CHANGES;
```

This implicit behavior can then be deprecated in a future release, giving users time to adjust their workloads to use the `EMIT CHANGES` clause wherever necessary. Note that *transient* queries will not implicitly include an `EMIT CHANGES` clause. By their nature, transient queries are generally not part of a deployable workload and therefore are unlikely to be problematic in terms of backward incompatibility.

## Costs

Beyond the engineering work required to support queryable state stores, another significant cost of changing the syntax for streaming queries is that we will need to update all documentation, tutorials, etc. to reflect the `EMIT CHANGES` syntax.

## Future work

The initial implementation of queryable state stores will add the ability to execute pull queries on materialized views. However, there are other query forms that we may consider supporting in the future that our minimal syntax addition leaves room for:

* Pull queries on streams
* Terminating queries that include incremental changes
* Continuous queries that only output final results without incremental changes (e.g. suppressed output for windowed aggregations)

---

## Other considered approaches

The following are other approaches that have been considered for identifying point-in-time versus streaming queries:

### **Interpret any query against a table as pull**

The table-based approach would consider any SQL query against a KSQL table as a pull query. Any query against a stream would then be a streaming query. The primary issue with this approach is that it complicates the capability of running a streaming query on a table, which is a highly valuable use case. Also, since pull queries would be identified somewhat implicitly, this approach may not be particularly intuitive to users. It would also make it relatively easy to write a "correct" query that does the wrong thing.

### **`SELECT STREAM[ING]`**

Streaming queries would be identified by a new `STREAM[ING]` keyword: `SELECT STREAM[ING]...`. Similarly to `EMIT STREAM`, `SELECT STREAM[ING]` becomes somewhat redundant in the context of CSAS:

```sql
CREATE STREAM s0 AS SELECT STREAMING * FROM s1;
```

This approach also doesn't facilitate de-emphasizing the stream abstraction should we decide to eventually make KSQL more table centric.

### **`WHERE` clause based**

With this approach, streaming versus pull behavior would be identified using a query's `WHERE` clause. For example,

```sql
SELECT * FROM  rel WHERE ROWTIME >= BEGINNING()
```

One drawback of this approach is that it may become a bit cumbersome with more complex queries such as `JOINs`:

```sql
SELECT * FROM  rel0 JOIN rel1 ON rel0.x = rel1.x
  WHERE rel0.ROWTIME >= BEGINNING() AND rel1.ROWTIME >= BEGINNING();
```

Furthermore, specifying streaming versus pull behavior in this way is somewhat implicit and thus may not be totally intuitive to users. The above query could potentially be interpreted by a user to mean, "give me all the rows from the join of these tables and return the final result when the query is complete." In other words, it can reasonably be interpreted as a pull query.

### **Separate streaming and point-in-time interaction modes**

The interaction mode approach would use a configuration parameter (`ksql.client.mode`), settable within the session, KSQL server configuration file, JDBC connection properties, CLI etc. Any query run with `ksql.client.mode` set to `streaming` would yield streaming results. Any query run with `ksql.client.mode` set to `interactive` would yield point-in-time results. The reasoning behind this approach was that any KSQL connection/session that is already running a streaming query can't be used for anything else anyways. Interleaving streaming and pull queries from an application would require separate sessions/connections/requests, so explicitly setting the interaction mode in these cases would solve the problem of streaming versus point-in-time differentiation.

However, this approach really breaks down in the context of the CLI experience. Making it as easy as possible for users to rapidly prototype and experiment with different queries is of critical importance to us. Requiring users to repeatedly set the client interaction mode before running different kinds of queries would just be too burdensome, on a critical usability path.
