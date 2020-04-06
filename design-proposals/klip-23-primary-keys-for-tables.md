# KLIP 23 - Use PRIMARY KEYs for tables

**Author**: @big-andy-coates |
**Release Target**: TBD |
**Status**: In Discussion |
**Discussion**: TBD

**tl;dr:** Tables in SQL have `PRIMARY KEY`s. In ksqlDB, Tables, like Streams currently have only
`KEY` columns, i.e. columns that come from the Kafka message key. We should introduce `PRIMARY KEY`
syntax for tables as this is standard SQL syntax and will help highlight the difference in semantics
for a table's primary key vs a stream's key column.
           
## Background

### KEY keyword

A release or two ago ksqlDB introduced the `KEY` keyword to allow users to specify the type of the
`ROWKEY` column in their `CREATE TABLE` and `CREATE STREAM` statements. For example:

```sql
-- table with BIGINT key
CREATE TABLE USER (ROWKEY BIGINT KEY, NAME STRING, ID BIGINT) WITH (...);

-- stream with string key
CREATE STREAM CLICKS (ROWKEY VARCHAR KEY, AGENT VARCHAR) WITH (...);
```

### ksqlDB 'any key-name' enhancement

There is currently [work afoot](https://github.com/confluentinc/ksql/issues/3536) to remove the
restriction that the key column must be named `ROWKEY`. This work is mostly complete, but is
currently disabled by a feature flag, waiting on reviews.

With this change a table, or stream, can give any name to its key column. This effectively makes the
`WITH(KEY)` syntax redundant.  For example:

```sql
-- old style statement (where the ID value column is an alias for ROWKEY)
CREATE TABLE USER (ROWKEY BIGINT KEY, NAME VARCHAR, ID BIGINT) WITH (KEY='ID', ...);

-- new style statement:
CREATE TABLE USER (ID BIGINT KEY, NAME VARCHAR, ID BIGINT) WITH (...);
```

Removal of the `WITH(KEY)` syntax is benefical as it removes the need for users to duplicate the key
into a value column, which can require pre-processing of data.

## Motivation

### SQL Tables have PRIMARY KEYs

The existing `KEY` keyword can currently be used in `CREATE STREAM` and `CREATE TABLE` statements,
as shown above.

The keyword informs ksqlDB that the column should be loaded from the Kafka message's key, rather
than its value.

However, for tables, the key column is actually the PRIMARY KEY of the table. Hence we should use
the SQL standard `PRIMARY KEY` and not simply `KEY` when defining tables. For example,

```sql
CREATE TABLE FOO (ID INT PRIMARY KEY, NAME STRING) ...
```

Streams, which by definition do not have a primary key, should not use `PRIMARY KEY`.

This change should also make it clearer to users that a table's `PRIMARY` KEY is not the same as a
stream's `KEY`. The semantics of how stream and tables keys are processed by ksqlDB are different:

1. The SQL standard says the columns in a table's primary key must be NON NULL. Any NULL key is
dropped by ksqlDB. No such constraint exists for a stream's key column(s), which are treated much
like any value column.

2. The SQL standard also says that the combination of the columns in the primary key must be unique.
This is why ksqlDB can use a table's primary key to 'upsert' a table's changelog into a materialized
table. KsqlDB does not materialize streams.

## What is in scope

* A syntax only change, requiring tables to be defined with `PRIMARY KEY`, instead of `KEY`.
* Only syntax for a single PRIMARY KEY.

## What is not in scope

* Any functional change in key handling semantics. The proposed change is purely syntactical.
* Multiple key columns. Syntax for composite primary keys is well documented in the SQL community
and will be introduced with the work to support multiple key columns.

## Value/Return

Main gains:

1. More standard SQL: `PRIMARY KEY` is standard sql syntax, where as `KEY` is not.
2. Should help differentiate the difference between a stream's key column and a table's primary key
to users.

There is also some discussion going on about how ksqlDB should model streams: should they continue
to be their own collection type, or should they be modelled as tables without primary keys? If we
do go the latter route it will be crucial that we differentiate `PRIMARY KEY`s from non-primary key
columns.

## Public APIS

`CREATE TABLE` statements will change from using the `KEY` keyword for key columns to `PRIMARY KEY`.
For example:

```sql
-- current syntax:
CREATE TABLE FOO (ID INT KEY, NAME STRING) ...

-- proposed syntax:
CREATE TABLE FOO (ID INT PRIMARY KEY, NAME STRING) ...
```

Note: `CREATE STREAM` statements will not be affected.

## Design

This is purely a syntax change.  All that is needed is:

 1. add an optional `PRIMARY` keyword to be added to the syntax of create statements, and
 1. to have the parser reject any `CREATE TABLE` key columns _without_ the `PRIMARY`
keyword, and reject any `CREATE STREAM` key column _with_ the new keyword.

## Test plan

QTT tests will be updated to reflect the new syntax for tables. Existing historic tests will ensure
the change is backwards compatible.

## LOEs and Delivery Milestones

Change already implemented: https://github.com/confluentinc/ksql/pull/4986.

## Documentation Updates

See existing PR for doc updates: https://github.com/confluentinc/ksql/pull/4986

This new syntax will be highlighted in a blog post, (tracked by
https://github.com/confluentinc/ksql/issues/4960). The existing examples in the ksqlDB repo and the
examples repo will need updating with the new syntax,
(tracked by https://github.com/confluentinc/ksql/issues/4927).

Where out of data examples are used in the new version of ksqlDB, the error message will be very explicit about what needs to be changed, e.g. 

```
ksql> CREATE TABLE FOO (ROWKEY INT KEY, NAME STRING) WITH (...);
Error: Line: 1, COL: 18: `KEY` used in table schema. Tables have primary keys. Please replace KEY with PRIMARY KEY in: ROWKEY INT KEY
```

## Compatibility Implications

None

## Security Implications

None
