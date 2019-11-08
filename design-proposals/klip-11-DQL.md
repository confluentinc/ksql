# KLIP 11 - Redesign KSQL query language

**Author**: Matthias J. Sax, mjsax | 
**Release Target**: 5.5+ | 
**Status**: In Discussion | 
**Discussion**: _link to the design discussion PR_

**tl;dr:** [KLIP-8](klip-8-interactive-queries.md) adds the ability to query table state similar to RDBMS style queries.
           To allow queriying tables in a "streaming manner", i.e., receive a table's changelog stream as query result
           the new keyword `EMIT CHANGES` is introduced.
           However, adding this new keyword seems not to be an holistic solution.
           We propose to resedign the query language to allow a more native way to support different types of queries
           over streams and tables.
           The proposed design is based on the stream-table dualtiy idea, and tries to embed this duality
           into the query language itself.
           
## Motivation

KSQL and its runtime engine Kafka Streams are built on the idea of the stream-table duality.
Currently, KSQL does exploit this duality only in a limited form:

* A topic can be consumed as a changelog stream into a TABLE
* A query result that is a TABLE, can be materialized as a changelog stream into a topic

Furthermore, KLIP-8 introduces "pull queries" to allow querying TABLE state similar to RDBMS style queries.
To allow for "push queries", KLIP-8 introduces the `EMIT CHANGES` clause that does not align seamlessly to the stream-table duality idea.

In this KLIP, we propose the "Dual Query Language" (in alignment to the ["Dual Streaming Model"](https://dl.acm.org/citation.cfm?id=3242155)
that puts forward STREAMS and TABLES as first class citizens and exploits the stream-table duality natively.
We aim to allow the seamless transformation of streams and tables into each other,
to express different types of queries over both with intuitive syntax and semantics.



## Background: Query Properties and Design Space

Currently, KSQL supports TABLES but does not support INSERT/UPDATE/DELETE statements on those tables.
The reason for this design is the fact, that TABLES in KSQL are actually MATERIALIZED VIEWS,
that are backed by a continuous query that updates the view.

### Tables vs. Materialized Views

Tables and materialize views (short MV) are quite similar but still different.
An important observations is, that the difference between both is only on their _write path_, but not on their _read/query_ path.
A RDBMS style table is rather "static" (but not immutable) as it's only updated with explicit insert/update/delete statements.
On the other hand, a materialized view is derived from some base data (either a stream or a table in KSQL)
and continuously updated when the base data changes.
Nevertheless, on the read path both look and behave the same: we can query their current state, both are updated,
and we can apply the stream-table duality idea on both to get a changelog stream from each.
To design a query language, it is not relevant how or how frequently a table/MV is updated
and thus we don't need to distinguish both on the query input path.

### Query Types

KSQL aims to support many different types of queries and those queries have multiple different properties.
We need to clearly distinguish those query properties before we can start with a detailed discussion what type of queries we want to support.

We suggest to distinguish the following query properties:

* considers a single point in (event) time vs considers a (potentially finite or infinite) time range
* self-terminating or runs "forever" (i.e., until terminated by a user)
* query input types; note, we extend the discussion to arbitrary n-ary queries, even if KSQL only supports unary and binary queries atm
  (why we consider only those two categories gets clear later):
  * all inputs are tables
  * at least one input is a stream
* we don't consider the output type, because it's not a property of the query itself, but derived by the design of query language
* we don't consider transient (client/CLI) queries vs persistent queries (CSAS, CTAS) because both should have the same semantics

Note, that while we consider those properties as independent in general, there are some corner case for which some properties contradict each other,
i.e., there are some partial dependencies. More details below.

Before we dive into a detailed discussion, we lay out (in less technical terms) what type of queries we want to support (this is for sure an incomplete list):

* forever running query over an input STREAM that returns an output stream
  * think: simple stream filter, stream-table join, or stream-stream join
  * this may be a transient client query or a persistent query
* forever running aggregation query over an input STREAM that returns an output table
  * think: a persistent continuous query (may be windowed)
  * for a persistent query, this should populate a MV
  * for a transient client query, we would like to get the corresponding changelog stream
* a RDBMS style query against a TABLE that terminates automatically and returns a table (called 'pull queries' in KLIP-8)
  * conceptually, we can think of everything that 'classic' SQL allows
* a self-terminating query over an input STREAM that returns an output stream
  * think of a query with a range in time, e.g., yesterday, or everything up to "now" (but don't wait for future events)
* a forever running query over an input TABLE that return a changelog stream
  * answer my query on the current table state, and keep updating the answer if the base table state changes

### Baseline Assumptions

* Data stored in Kafka topics is just data; there is no semantic interpretation attached
* A _STREAM_ is an unbounded append only-sequence of immutable facts
* A _TABLE_ is a mutable bounded set
  * for each point in (event) time, there exist exactly one TABLE _version_ (i.e., a snapshot)
  * a TABLE _evolves_ over (event) time (we call it an _evolving table_) when new insert/updates/deletes are applied
    * a TABLE version is "complete" if all updates for the corresponding event time are applied
    * updating a TABLE from version v to v+1 might imply multiple updates and might take some time,
      hence, at any point in wall-clock time a TABLE might be in an inconsistent state
      (we call those the _generation_ of a table, i.e., for each table version, there might be multiple generation and only the last generation is consistent).
  * a TABLE can be represented as (versioned) state or changelog – we consider the physical representation as an implementation detail



## What is in scope

The focus is to remove `EMIT CHANGES` and introduce new syntax to support different types of queries over streams and tables.
We want to exploit the stream-table duality and to transform STREAMS and TABLES into each other seamlessly.
Furthermore, the query language should express the difference between TABLES and MATERIALIZED VIEW explicitly.

## What is not in scope

This KLIP proposes a redesign of the query language but does not propose to implement all possible features right away.
The intent is to make the language "future proof" such that new feature can be added seamlessly without breaking the language.
In particular, we should not implement (cf. "Design" section below):

* re-introduce `CREATE TABLE` statement to create RDBMS style tables that allow insert/update/delete statements
* allow for complex queries over table state, e.g., aggregation and joins
* we might not need to implement the new `AS OF <timestamp>` clause in the first iteration
* we might not need to support terminating queries over streams right away

We limit this KLIP to not introduce too much syntactic sugar to express queries.
This may make some queries unnecessary "complex/clumsy" but the goal is to illustrate to underlying principles of the language.
We can add syntactic sugar after we agree on the basic design of the language.



## Value/Return

The "Dual Query Language" should be easier to understand and easier to use.
If will allow to support nested queries seamlessly and unifies query semantics (persistent vs transient queries).
Furthermore, the language is designed to be "future proof", i.e., it considers potential future features and thus allows
to extend the capabilities of KSQL without the need to change the language in a breaking way.



## Public APIS

We propose the following language changes:

* introduce `TABLE(<stream>)` operator: takes a STREAM as input, and changes its semantic interpretation from facts to updates; it returns a TABLE
  * note: the returned TABLE is not an RDBMS style table, but rather a MV
  * note: it's not specified if the returned TABLE is materialized into a state store or represented as a changelog
    (it's up the the KSQL optimizer or KS runtime to make a materialization decision)
  * the `TABLE()` operator consumes it's input STREAM "from beginning"
  * we could extend `TABLE()` to also accept a TABLE as input if it makes the language smoother;
    it's obvious that `TABLE(<table>)` would be idempotent, i.e., `TABLE(t) = t`
* introduce `STREAM(<table>)` operator: takes a TABLE as input, and returns a STREAM that contains a record for each update to the TABLE
  * note: the returned STREAM is a fact stream, and would be processed with corresponding STREAM semantics (it's semantically not a changelog stream)
  * note: the input TABLE can either be an RDBMS style table of a MV
  * by default, the output STREAM contains one record for each input table record (full table scan on the table generation when the query was issues)
    plus a record for each input table update
  * we could extend `STREAM()` to also accept a STREAM as input if it makes the language smoother;
  it's obvious that `STREAM(<stream>)` would be idempotent, i.e., `STREAM(s) = s`
* Introduce an `AS OF <timestamp>` clause for RDBMS style queries against tables
* Introduce `CREATE MATERIALIZED VIEW` statement
* Deprecate `CREATE TABLE` statement (in favor of the new `CREATE MATERIALIZED VIEW` statement) and eventually remove it
* Deprecate `EMIT CHANGES` clause

Semantic changes:

* An aggregation query over a stream returns a stream (and not a table)

Naming:

* Drop the terms `pull query` and `push query`
* Use the terms `table query` and `stream query`
  (this will also allow to use the phrase "query a stream" and "query a table" in a straight forward way)



## Design

We introduce new syntax step by step and illustrate it by example. We also explain the corresponding semantics.


**Example 0: create persistent streams/tables**

Syntax is up for discussion (it's possible to merge some statements) – put out multiple for brainstorming.

```sql
-- no change to existing `CREATE STREAM` statements
CREATE STREAM <name> FROM <topic>

CREATE STREAM <name> AS <query>   --the query must return a STREAM



-- new syntax that replaces existing `CREATE TABLE`
CREATE MATERIALIZED VIEW <name> FROM <topic>

CREATE MATERIALIZED VIEW <name> FROM <stream>   ---the <stream> must be a registered stream from the catalog

CREATE MATERIALIZED VIEW <name> AS <query>   --the query must return a STREAM

-- the last two statements could be unified, because the <query> must return a STREAM anyway
--   => there is no need to distinguish if a persistent or temporary STREAM is upserted into the result TABLE



-- maybe in the future
CREATE TABLE <name> WITH <schemaDefinition>

CREATE TABLE <name> AS <query>   --the query must return a TABLE
```

Examples and detailed description:

```sql
CREATE STREAM myStream FROM topicA
-- creates a persistent STREAM
-- same a current KSQL
```

```sql
CREATE MATERIALIZED VIEW myTable FROM topicB
-- create a persistent TABLE (that is continuously updated; insert/update/delete statement are not allowed)
-- (for backward compatibility, CREATE TABLE would we equivalent, but in deprecation mode until its eventually removed)
```

```sql
CREATE MATERIALIZED VIEW myTable FROM myStream   -- 'myStream' is a registered stream name from the catalog
-- create a persistent TABLE (that is continuously updated; insert/update/delete statement are not allowed)
-- this is a new type of statement that implicitly "casts" a fact stream into an update stream and upserts it into the result TABLE
-- maybe we would need to allow the same with old CREATE TABLE syntax?
```

```sql
CREATE MATERIALIZED VIEW myTable AS SELECT * FROM myStream
-- basically the same as previous example; we just upsert a temporary stream into the result table
-- it's unclear if we should use `FROM <stream>` in the previous example:
--   the current argument is, that there is no query on the right hand side and thus `FROM` is better than `AS`
--   (one could argue though, that both example are effectively the same as both return a STREAM and there is no need to have `AS <query>`
--   because the grammar could be `FROM <stream>` with `<stream> ::= [<registredStreamName> | <query>]`
--   (we should discuss those grammar details eventually, but it's not important to the core idea of the language design)
```

```sql
CREATE STREAM myChanglog AS SELECT * FROM STREAM(myTable)   -- or should it be `AS` instead of `FROM`
-- or shorter
CREATE STREAM myChanglog FROM STREAM(myTable)   -- or should it be `AS` instead of `FROM`
-- creates a persistent STREAM
-- the prefix is not different to the existing CSAS statement in KSQL - the new stuff is the STREAM(...) operator that create a changelog stream from its input table
-- The output stream contains one record for each record in the table (ie, full table scan) based on the table generation
--   when the query was deployed plus a record for all future updates (we discuss later how the "start point" of the query can be changed,
--   i.e., start the query from an older table version)
-- note: the output is fact stream!
-- the first statement above shows the full syntax `CSAS <query>` while the second one is potential syntactic sugar for the same thing -
--   the same grammar question as above (ie, CREATE MV statement) raises (again, not important to the core design we want to discuss here)
```

Maybe in the future:

```sql
CREATE TABLE rdbmsStyleTable <withSomeSchemaSpec>
-- create an empty persistent TABLE that is only modifies via insert/update/delete statements
-- currently not supported by KSQL
```

```sql
CREATE TABLE rdbmsStyleTable AS SELECT * FROM someTableOrView -- note, it does not make a difference if the input to the right hand side SELECT-query is a "table" or a MV
-- create a table with initial state being the result of the query (same as a RDBMS query)
```


**Example 100: a stateless stream query**

```sql
[CREATE STREAM resultStream AS] SELECT * FROM inputStream
-- returns a STREAM
-- a client can issue a transient query to get an infinite result stream it can iterator over (until the client terminates the query) or
-- the result stream can be persistent into an output STREAM (that is backed by a topic)
-- same as current KSQL syntax (we can add filters, projections, etc)
-- the query starts to consume inputStream for "beginning" and does not terminate
```

The last point is a design principle in our language:

* (P1) a new query over an input stream by default starts to query the input stream from the beginning
* (P1-a) a user can set a different start point with a filter predicate, e.g., `ROWTIME > NOW()` (more details later)
* (P1-b) a user can define a "termination point" with a filter predicate, e.g., `ROWTIME < NOW() + 1 DAY` (more details later)
  * It's open for discussion, if a "termination query over an input stream" can be a persistent query or not
    (ie, should we only support transient terminal queries?)
  * My personal take is: we can allow both types, because we already have `INSERT INTO myStream` and we can generalize it –
    a STREAM may or maynot have a continuous query that write into it – at the same time, a STREAM may have multiple CQ that write into it,
    for which the writes are non-deterministically interleaved – IMHO, it's even "ok" to write into a STREAM from "external"
    by writing into the corresponding topic (it's risky though to not break the schema...) –
    of course, uses need to be aware of what they are doing, but I advocate for maximum flexibility


**Example 150: stream-table join**

```sql
[CREATE STREAM resultStream AS] SELECT * FROM inputStream s JOIN inputTable t ON s.key = t.key
-- same properties as for Example 100
-- because one input is a stream, the output is a stream
```

The last point is a design principle in our language:

* (P2) a query with at least one input STREAM, returns a result STREAM
* (P2-a) we can extend this principle to (stream-table)-table, stream-(table-table) and (stream-stream)-table joins (and any other n-ary query)

The reason for this design principle is, that the "infinite" nature of an input stream dominates the "finite" nature of a table –
if a query has streams and tables mixed in the input, the provided input tables don't provide any means to terminate the query automatically,
but the query still needs to process the infinite input stream and thus produces an infinite output stream.


**Example 170: stream-stream join**

```sql
[CREATE STREAM resultStream AS] SELECT * FROM inputStream s1 JOIN otherStream s2 ON s1.key = s2.key WITHIN 1 HOUR 
-- same properties as Example 150
```


**Example 200: a stateless table query**

```sql
[CREATE TABLE myRdbmsStyleTable AS] SELECT * FROM inputTable
-- returns a TABLE
-- a client can issue a transient query to get a finite result table "snapshot" (based on the current table generation) or
-- the result table may be persistent via CREATE TABLE statement (current not allowed in KSQL: note, this would create a RDBMS style table,
--   not a MV, and hence after the TABLE is created, insert/update/delete statement could be allowed):
--   in particular CREATE TABLE would create an empty table, and the AS part would load the query result into the new table;
--   we might add this the persistent query version only in the future
-- a KLIP-8 "pull query" (we can add filters, projections etc)
-- the query conceptually "scans" the full inputTable (in particular the current generation when the query is issued),
   computes the finite result, and terminates
```


**Example 250: a table-table join**

```sql
[CREATE TABLE myRdbmsStyleTable AS] SELECT * FROM inputTable t1 JOIN otherTable t2 ON t1.key = t2.key
-- returns a TABLE
-- same properties as Example 200
-- because all inputs are a table, the output is a table
```

The last point is a design principle (cf. P2 above)

* (P3) a query with only TABLES as inputs, returns a TABLE as output
* (P3-a) we can extend this principle to n-ary TABLE joins

An open question is, what table version/generations are join to each other for this case – for unary queries,
we can just use the "current generation of the current version" (i.e., whatever is in RocksDB atm when the query is issued).
For n-ary queries, we need to figure out what semantics we want to have/allow (if we ever support this type of join to begin
with—--currently, I would advocate to not allow this query at all, but tell users to define a continuous table-table join query with proper
event-time semantics and query the result TABLE/MV instead).


**Example 300: creating a materialized view (persistent continuous query of an input table)**

```sql
CREATE MATERIALIZED VIEW resultView AS SELECT * FROM inputTable
-- returns a TABLE
-- this is always a persistent query (without the CREATE MV AS prefix, the query is the same as Example 200 – compare Example 300 for the transient version)
-- the result is a MV, ie, a table with a continuous queries that update the table when the base data changes; insert/update/delete statements are not allowed
-- the same as current CTAS; for backward compatibility, current CTAS would still be supported but would be deprecated
```


**Example 400: subscribing to a changelog stream (transient continuous query of an input table)**

```sql
[CREATE STREAM myChangelog AS] SELECT * FROM STREAM(tableOrMaterializedView)
-- returns a STREAM
-- compare Example 100 (those properties apply)
-- without CSAS prefix, it is the transient query counterpart to Example 300 – all updates are "streamed" continuously to the client
--   (note, that the result is a fact-stream and it's the client responsibility to "interpret" the records with update semantics)
-- The STREAM(...) operator creates a changelog stream from its input TABLE (can be a RDBMS style table or a MV); compare Example 0
-- The output stream contains one record for each record in the table (ie, full table scan) based on the table generation when the query was issued
--   plus a record for all future updates
```


**Example 500: aggregating a stream**

```sql
[CREATE MATERIALIZED VIEW resultView AS] SELECT count(*) FROM inputStream GROUP BY <some-grouping-condition>
-- returns a STREAM (!!!): different and not different to current KSQL semantics... compare below
-- a "CREATE MV" statement takes an input stream, re-interprets the stream as updates,
--   and create a table/mv from it (hence, the result of the SELECT-stmt must be a stream)
-- if the query is issued as transient query, the user get the changelog stream of the aggregation result (as a fact-stream)
-- even if the return type of the query is changed to a STREAM, from a user perspective nothing change...
--   the user sees the same result for a persistent or transient query (ie, it's more or less an internal semantic change,
--   but a change that we can hive from the user)
-- note: if the aggregation is windowed, we need to allow to set a retention time on the created MV (how retention times are applied and "propagated"
--   is an important discussion we need to have – it's not covered in this KLIP)
```

Example 500 illustrates our design principle P2 for aggregation queries! Queries over input streams return output streams.


**Example 510: aggregating a stream and getting the changelog**

```sql
[CREATE STREAM myChangelog AS] SELECT count(*) FROM inputStream GROUP BY <some-grouping-condition>
-- returns a STREAM (!!!)
-- the actual query is the same as in Example 500 – however, because the query returns a STREAM (in contrast to current KSQL)
--   we can simply create a persistent query result from the result stream or let a client retrieve the result stream in a transient manner
--   (note how Example 500 and 510 are exploiting the stream-table-duality seamlessly)
```


**Example 600: aggregating a table**

```sql
[CREATE TABLE resultTable AS] SELECT count(*) FROM inputTable GROUP BY <some-grouping-condition>
-- returns a TABLE
-- same as filter-query (cf. Example 200)

```


**Example 610: aggregating a table into a MV**

```sql
CREATE MATERIALIZED VIEW resultTable AS SELECT count(*) FROM inputTable GROUP BY <some-grouping-condition>
-- returns a TABLE
-- same as mv-query (cf. Example 300)
-- the query above uses in implicit "cast" as syntactic sugar; CREATE MV statement takes a STREAM as input
--   but the right hand side query returns a TABLE; the full syntax would be:
--     `CREATE MATERIALIZED VIEW resultTable AS SELECT * FROM STREAM(SELECT count(*) FROM inputTable GROUP BY <some-grouping-condition>`
--     or shorter
--    `CREATE MATERIALIZED VIEW resultTable AS STREAM(SELECT count(*) FROM inputTable GROUP BY <some-grouping-condition>`
```

(We need to discuss if STREAM(table) itself should be a valid query or not...)


**Example 620: getting the changelog of a table aggregation query**

```sql
STREAM(SELECT count(*) FROM inputTable GROUP BY <some-grouping-condition>)
-- returns a STREAM
-- same as changelog-query (cf. Example 400)
-- the explicit STREAM() keyword is required because the inner query returns a TABLE
-- the shown query uses syntactic sugar notation for the full query (cf Example 610 above):
--   `SELECT * FROM STREAM(SELECT count(*) FROM inputTable GROUP BY <some-grouping-conditions>)`
```


#### On Table queries and Materialized Views:

In the example above, we actually introduced some syntactic sugar already. The concrete syntax of a create-materialized-view statement is

```sql
CREATE MATERIALIZED VIEW FROM <stream>
```

Hence, a query that returns a TABLE, cannot be passed into the statement, and the "correct" query with full syntax would we:

```sql
CREATE MATERIALIZED VIEW FROM SELECT * FROM STREAM(SELECT * FROM table)
```

However, we propose to have an implicit table-to-stream cast and allow the shorter version as used in the examples above:

```sql
CREATE MATERIALIZED VIEW FROM STREAM(SELECT * FROM table)
-- or even shorted (note that the change to use `CREATE MATERIALIZED VIEW` instead of `CREATE TABLE` make the semantics clear; it also align to RDBMS syntax)
CREATE MATERIALIZED VIEW FROM SELECT * FROM table
```

Similarly, we can simplify Example 400 from

```sql
[CREATE STREAM myChangelog AS] SELECT * FROM STREAM(tableOrMaterializedView)
```

to

```sql
[CREATE STREAM myChangelog AS] STREAM(tableOrMaterializedView)
-- or even allow to shortcut it with an implicit cast (for a persistent query only)
CREATE STREAM myChangelog AS/FROM tableOrMaterializedView
```


### The Dual Query Language

The basic syntax (simplified) of the language is as follows:

```sql
<query> := [<create-statement>] <select-statement>

<create-statement> := CREATE (STREAM | TABLE | MATERIALIZED VIEW) name (AS <query> | FROM <topic> | WITH <schemaDefinition>]
<select-statement> := STREAM(<select-statement)
                      | TABLE(<select-statement)
                      | SELECT <projection> FROM <inputs> WHERE <filters> GROUP BY <grouping> WINDOW BY <windowing> HAVING <having>;

-- note: a <select-statement> returns a TABLE if all inputs are TABLES;
--       otherwise (ie, if there is at least one input STREAM), it returns a STREAM
<inputs> := <stream> | <table> | <binary-join-statement> | <select-statement>

```

The most important design principle is, that input-streams result in output-streams, and input-tables result in output-tables.
This implies that by default, a query over an input stream is continuous while a query over an input table is terminal.
Hence, we propose to introduce the terms **"stream query"** and **"table query"** to distinguish the two main categories of supported queries.

* **stream query** (continuous query, streaming query, push query)
  * input stream (at least one)
  * output stream
  * by default, runs forever (we discuss below how a streaming query can be terminal in more detail; i.e., time-range/interval queries)
  * considers a range in event-time (may be finite of infinite)
* **table query** (point-in-time query, lookup query, snapshot query)
  * input table (all)
  * output table
  * terminal
  * considers a single point in event-time (we need to figure out the details about table generations, but IMHO,
    the query should be answered based on the latest generation for the particular point in event-time)

Note, that the actual terminology is base on the query **input and output type**! For example, the **table** query

```sql
SELECT * FROM myTable
```

has an input and output table. If the query is "converted" into a **stream** query via

```sql
SELECT * FROM STREAM(SELECT * FROM myTable)
```

we have two queries: and inner table query and an outer stream query, and the stream query has a stream as it's input and output type.
Hence, even if we allow a shorter notation as syntactic sugar:

```sql
STREAM(SELECT * FROM myTable)
```

it's still two nested queries and not one query with input table and output stream (one could think of the output type as the determining factor,
if a query is called a stream or table query—however, that is somewhat a shortcut and might lead to confusion).



#### Terminal Stream Queries:

While a table query is always terminal, a stream query is continuous by default but may be terminal.
The below examples illustrate how a custom start and end point for stream queries can be specified base on either time or offset:

```sql
-- setting custom starting point
SELECT * FROM stream
  WHERE ROWTIME >= _someTimestamp_
        [AND|OR <some filter conditions over the attributes>];

-- we need to discuss how we handle partitions; the idea atm is, to apply the same offset to all partitions
-- (it's questionable how useful this is, but I think we should allow it anyway)
SELECT * FROM stream
  WHERE OFFSET >= _someNumber_
        [AND|OR <some filter conditions over the attributes>];

-- setting custom end point (this defines a terminal stream query)

-- note, this will terminate when event time reaches current wall-clock time
--   => this might not be intuitive, but also not too hard to explain to users
SELECT * FROM stream
  WHERE ROWTIME <= now()
        [AND|OR <some filter conditions over the attributes>];

-- terminates when partition end-offset is reached
--   => is this query what some people mean by a "point in time over stream" query ???
SELECT * FROM stream
  WHERE OFFSET <= end()
        [AND|OR <some filter conditions over the attributes>];


-- join
-- setting custom starting and end point is the same as above with corresponding `stream1.` or `stream2.` prefix
SELECT *
  FROM stream1 JOIN stream2
    WITHIN _someTimeWindow_
    ON [join-condition]
  WHERE <some filter conditions over the attributes>];
```

Open questions:

* Should we allow persistent, terminal, stream queries?
  * I think yes (already mentioned in the beginning)
* It seems that `ROWTIME <= now()` semantics might not be intuitive as it mixes event-time and wall-clock time:
  * I would expect that people often just want to stop at the end of the topic and confuse both concepts
 * can we do better?
* Is `OFFSET <= end()` too clumsy to express what many people may want, i.e., terminate at the end of the topic?
  * can we do better?
* For the ROWTIME termination criteria, it's unclear how to handle out-of-order data
  * I think we can add a `WITH GRACE PERIOD OF <someTimePeriod>` to cover this case though
    * How would we handle data with larger timestamp than specified ROWTIME bound; **proposal:** those record should be filtered out
    * If a user wants to "read over" and not filter out out-of-order data, the predicate should be `OFFSET < offsetByTimestamp(endTime + gracePeriod)`
  * By default, we would terminate on the first record with larger timestamp than specified
* For the ROWTIME start point criteria, how should out-of-order data be handled?
  * **proposal:** out-of-order data with smaller timestamp than specified should be filtered out
 * if a user does not want to filter out-of-order data, but basically wants to start from an offset given a timestamp,
   the predicate should be `OFFSET >= offsetByTimestamp(_someTimestamp_)`
   * for this case, we would set a different "start offset" for each partition what is different to `OFFSET >= _someNumber_` though...

Note, that the proposed stream query semantics align with the current KSQL semantics and thus there are no backward compatibility concerns.
The only difference is, that a transient query might actual terminate if a corresponding ROWTIME/OFFSET predicate is specified.


#### Query "old" Table State:

It might be desired to query old table state, or to start a streaming query (i.e., a query that returns a changelog stream) in the past.
For tables, offset based starting points do not seem to make sense compared to input streams.
Furthermore, because a table query starts to query the current table state by default and terminates,
it does not work to use a timestamp-based filter condition.
Hence, we propose in alignment to temporal SQL, to add as `AS OF` statement:

```sql
-- syntax
<table-query> ::= SELECT <projection>
                  FROM <table> [JOIN <table> ON <predicate>]*
                  WHERE <predicates>
                  GROUP BY <grouping>
                  HAVING <predicates>
                  [AS OF <timestamp>]

-- the given <timestamp> is a system-timestamp (ie, epoch in ms)
-- note that `AS OF <timestamp>` has event-time semantics!
--   - we should add some functions that allow to convert a date/time etc, eg, `AS OF timestamp("some string")`
--   - we should support expressions like `AS OF now() - 1 DAY`
-- possible values:
--    -> user specifies some long value explicitly (unlikely but possible; unclear how useful but should be supported IMHO)
--    -> current() -> returns the timestamp of the current table generation (default)
--    -> now()     -> returns current wall-clock time
--    -> latest()  -> returns the highest future event timestamp of the table    // this is a little fuzzy and we need to think hard to actually define it
--
-- current(): this is the default if `AS OF` is not specified to allow people to query the current generation, ie, whatever is in the table when the query what issues
--            we need to consider how/if we want to support querying TABLES that are not materialized in a state store (I would put this as a separate discussion though)
--
-- now(): this statement is tricky as it mixes wall-clock and event-time; if the table's event-time is smaller than current wall-clock time,
--        the query would "hang" until the table's event-time advanced accordingly (ie, to the wall-clock time when the query was issued) and terminate eventually;
--        this might be somewhat un-intuitive, but I think it's correct...
--
-- latest(): only useful if the TABLE is a MV; if the TABLE is an RDBMS style table, `latest() == current()`
--           TABLE is MV: for this case, the underlying input STREAM of the MV will be considered and the current log-end timestamp will be used;
--                        this is what people most likely want if they actually query a STREAM and want to upsert the result into a table,
--                        and get the "final" result, ie, the query terminates when the input topic is processed completely
--                        (with the definition of "completely" as: reached the end-offset when the query was started;
--                         ie, newly appended data after the query was started would not be contained in the result);
--           compare "Advance Query Patterns" below of intended use
--           // frankly, this idea is somewhat fuzzy and not sure if we can define the semantics in a sound way or if we need to find a different way to express this type of query



-- query table version for the given timestamp
-- note that _some_timestamp_ could conceptually also be in the future -- it's unclear if we would allow this or not and what the implications are
SELECT * FROM table AS OF _some_timestamp_;

-- query table version for a given timestamp and emit all update since then
-- the result stream contain a record for each row in the table as of <timestamp> plus all later updates
SELECT * FROM STREAM(SELECT * FROM table AS OF _some_timestamp_);

-- query table version for a given timestamp and emit _only_ updates since then
-- the result stream excludes the table state as of <timestamp> but only emits later updates
SELECT * FROM STREAM(SELECT * FROM table AS OF _some_timestamp_) WHERE ROWTIME > _some_timestamp_;

// note that for the last query above, both timestamps must be the same to get the desired result
// the query above is somewhat complex, but follow the principle that `simple queries should be easy, and complex queries should be possible`
// also pay attention that the ROWTIME filter is part of the outer stream query!
```


#### Advanced Query Patterns:

Given that stream and table queries can be nested arbitrarily, there is some interesting patterns uses can exploit. For example:

```sql
SELECT * FROM TABLE(SELECT count(*) FROM stream)
```

or short

```sql
TABLE(SELECT count(*) FROM stream)
```

This is a transient table query with a nested inner stream query. The inner stream query returns the full changelog stream of the aggregation,
however, a user might only be interesting in the "final" aggregation result and thus "upsert" the changelog stream into a table,
with the goal that the query terminates.
However, the outer `SELECT * FROM TABLE` uses an implicit `AS OF current()` query semantics, and thus would not return any rows, i.e., an empty table.
The reason for this gets clear, when we decompose the query into two parts:

```sql
CREATE MATERIALIZED VIEW result AS SELECT count(*) FROM stream

SELECT * FROM result AS OF current() 
```

After the first query get submitted, KSQL would start to process the input stream and populate the result TABLE.
If we query the state of the result TABLE directly afterwards, the query return whatever is in the TABLE when the query is submitted.
If we submit both parts as one query (as shown in the original nested query), we outer query would be evaluated on an empty table
because processing the inner query did not really start yet to process its input stream.

To get the desired result, the user must specify that the outer query should be answers only after the input stream is processed "completely"
(with the definition of "completely" as up the end-offset when the query was started). Hence, the use needs to add an explicit AS OF latest() clause:

```sql
SELECT * FROM TABLE(SELECT count(*) FROM stream) AS OF latest()
```

For this query, the outer table query waits until the inner stream query processed the full input stream.
It's a little fuzzy, how the `AS OF latest()` clause can be translated and executed cleanly though – it's more like a high level idea atm.
The goal is, to terminate the query if the input stream of the inner query is processed completely.
Note, that the following query would not achieve that:

```sql
SELECT * FROM TABLE(SELECT count(*) FROM stream WHERE OFFSET <= end())
```

The outer query would still be evaluate `AS OF current()` and still return an empty table.



## Test plan

* Use the new query syntax in existing tests.
* Test backward compatibility for deprecated `CREATE TABLE` statement and `EMIT CHANGES` clause

# Compatibility Implications

* As we want to replace the `CREATE TABLE` statement in favor of `CREATE MATERIALIZE VIEW` we should still support `CREATE TABLE` statement
  and print/log a warning that `CREATE TABLE` is deprecated.
* As we want to deprecate `EMIT CHANGES` clause, we should still support it and print/log a warning that `EMIT CHANGES` is deprecated.
* The semantic change that an aggregation query over an input stream now returns a stream instead of a table should not have any impact
  on users, as it's more an internal change and users won't notice
  (note that `EMIT CHANGES` is optional for this query for backward compatibility; cf. KLIP-8).

## Performance Implications

N/A

## Security Implications

N/A

