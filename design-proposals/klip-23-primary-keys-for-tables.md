# KLIP 23 - Use PRIMARY KEYs for tables

**Author**: @big-andy-coates |
**Release Target**: TBD |
**Status**: In Discussion |
**Discussion**: TBD

**tl;dr:** Tables in SQL have `PRIMARY KEY`s. Tables, like Streams, in ksqlDB currently have only
`KEY` columns, i.e. columns that come from the Kafka message key. We should introduce `PRIMARY KEY`
syntax for tables in ksqlDB as this is SQL syntax and to differentiate the semantics are different
for a table's primary key vs a stream's key column.
           
## Background

### KEY keyword

A release or two ago ksqlDB introduced the `KEY` keyword to allow users to specify the type of the
`ROWKEY` column in their `CREATE TABLE` and `CREATE STREAM` statements. For example:

```sql
-- table with BIGINT key
CREATE TABLE USER (ROWKEY BIGINT KEY, NAME STRING, ID BIGINT) WITH (...);

-- stream with string key
CREATE STREAM CLICKS (ROWKEY STRING KEY, AGENT STRING) WITH (...);
```

### ksqlDB 'any key-name' enhancement

There is currently [work afoot](https://github.com/confluentinc/ksql/issues/3536) to remove the
restriction that the key column must be named `ROWKEY`. This work is mostly complete, but is
currently disabled by a feature flag.

With this change a table, or stream, can give any name to its key column. This effectively makes the
`WITH(KEY)` syntax redundant.  For example:

```sql
-- old style statement (where the ID value column is an alias for ROWKEY)
CREATE TABLE USER (ROWKEY BIGINT KEY, NAME STRING, ID BIGINT) WITH (KEY='ID', ...);

-- new style statement:
CREATE TABLE USER (ID BIGINT KEY, NAME STRING, ID BIGINT) WITH (...);
```

## Motivation

### SQL Tables have PRIMARY KEYs

The existing `KEY` keyword can currently be used in `CREATE STREAM` and `CREATE TABLE` statements.
For example:

```sql
CREATE TABLE FOO (ID INT KEY, NAME STRING) ...
CREATE STREAM FOO (ID INT KEY, NAME STRING) ...
```

The keyword informs ksqlDB that the column should be loaded from the Kafka message's key, rather
than its value.

However, for tables, the key column is actually the PRIMARY KEY of the table. Hence we should use
the SQL standard `PRIMARY KEY` and not simply `KEY` when defining tables. For example,

```sql
CREATE TABLE FOO (ID INT PRIMARY KEY, NAME STRING) ...
```

Streams, which by definition do not have a primary key, should not use `PRIMARY KEY`.

The semantics of how stream and tables keys are processed by ksqlDB is different:

1. The SQL standard says the columns in a table's primary key must be NON NULL. Any NULL key is
dropped by ksqlDB. No such constraint exists for a stream's key column(s).

2. The SQL standard also says that the combination of the columns in the primary key must be unique.
This is why ksqlDB can use a table's primary key to 'upsert' a table's changelog into a materialized
table.

## What is in scope

* A syntax only change requiring tables to be defined with `PRIMARY KEY`, instead of `KEY`.
* Only single key column, which is what KSQL currently supports.

## What is not in scope

* Any functional change in key handling semantics. The proposed change is purely syntactical.
* Multiple key columns. Syntax for composite primary keys is well documented in the SQL community
and will be introduced with the work to support multiple key columns.

## Value/Return

Two main gains:

1. More standard SQL: `PRIMARY KEY` is standard sql syntax, where as `KEY` is not.
2. Should help differentiate the difference between a stream's key column and a table's primary key
to users.

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

This is purely a syntax change.  All that is needed is to add an optional `PRIMARY` keyword to
create statements and have the parser reject any `CREATE TABLE` key columns _without_ the `PRIMARY`
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

## Compatibility Implications

None

## Security Implications

None