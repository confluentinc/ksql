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

_How does your solution work?_

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

