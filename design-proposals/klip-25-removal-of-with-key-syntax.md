# KLIP 25 - Removal of WITH(KEY) syntax

**Author**: @big-andy-coates |
**Release Target**: 0.10.0; 6.0.0 |
**Status**: _Merged_ |
**Discussion**: [Github PR](https://github.com/confluentinc/ksql/pull/5117)

**tl;dr:** The `WITH(KEY)` syntax is the cause of much confusion and errors as it requires users to
have an _exact_ copy of the Kafka record's key in a field in the value. It allows users to provide
a more meaningful alias for the system `ROWKEY` column in their queries. Unfortunately, the
implementation of the `WITH(KEY)` feature is incomplete and buggy. With the introduction of the
feature that allows the key column to have any name, the main benefit of the `WITH(KEY)` feature has
been removed, leaving only the confusion and errors. We propose it should be removed.

## Motivation and background

The `WITH(KEY)` syntax allows a user to provide an alias for the `ROWKEY` system column in ksqlDB:

```sql
-- old skool syntax
CREATE TABLE INPUT (ID INT, NAME STRING) WITH ('KEY'='ID', 'KAFKA_TOPIC'='input', ...);
```

The above statement defines a table with an implicit STRING primary key column called `ROWKEY` and
two other columns called `ID` and `NAME`.  The presence of the `WITH('KEY'='ID')` tells ksqlDB that
the `ID` column is essentially an aliases for `ROWKEY` as it contains the exact same data.

### Aliasing

Allowing users to provide an alias for the system column `ROWKEY` allowed users to provide a more
domain driven name, making queries more expressive:

```sql
-- Given a stream of users where the key contained a region code:

CREATE TABLE USERS_BY_REGION AS
   SELECT COUNT(*) FROM USERS GROUP BY ROWKEY;

-- is less expressive than --

CREATE TABLE USERS_BY_REGION AS
   SELECT REGION, COUNT(*) FROM USERS GROUP BY REGION;
```

However, [a feature removing the restriction that key columns must be called `ROWKEY` is about to be
enabled](https://github.com/confluentinc/ksql/pull/5093). Once it is, the aliasing that `WITH(KEY)`
provides will be redundant, as the same can be achieved much more intuitively, and without the need
to duplicate the key data into a value field, using the new syntax. For example:

```sql
-- old skool syntax:
CREATE TABLE USERS (ROWKEY BIGINT PRIMARY KEY, NAME STRING, ID BIGINT) WITH ('KEY'='ID', ...);

-- new skool syntax:
CREATE TABLE USERS (ID BIGINT PRIMARY KEY, NAME STRING) WITH (...);
```

### Optimisations

This aliasing allows ksqlDB to optimize certain queries by avoiding unnecessary repartition steps,
should the 'key field' be used in a `PARTITION BY`, `GROUP BY` or `JOIN ON` clause. For example:

```sql
-- given:
CREATE TABLE INPUT (ID INT, NAME STRING) WITH ('KEY'='ID', 'KAFKA_TOPIC'='input', ...);

-- no repartition necessary as `ID` is equivalent to `ROWKEY`
SELECT ID, COUNT() FROM INPUT GROUP BY ID;
```

KsqlDB can avoid an unnecessary repartition step in the above statement as `ID` is equivalent to
`ROWKEY`, and grouping by `ROWKEY` is already correctly partitioned.

It's not just streams and tables created by `CREATE STREAM` and `CREATE TABLE` statements that can
track these `key-fields`. KsqDB also tries to keep track of the key field in derived sources
too, for example:

```sql
CREATE TABLE FOO AS SELECT ID, COUNT() FROM BAR GROUP BY ID;
-- Foo has key-field 'ID'
```

In the above, KsqlDB will detect that `FOO``s `ID` column is equivalent to `ROWKEY`. However, it can
only handle a single field. If there are multiple, only the first is tracked, for example:

```sql
CREATE TABLE FOO AS SELECT ID AS ID1, ID AS ID2, COUNT() FROM BAR GROUP BY ID;
-- Foo has key-field 'ID1'
-- `ID2` is not a key field, even though its a duplicate of `ID1`
```

Unfortunately, the implementation of the handling of these 'key fields' is very buggy, full of edge
cases and incomplete. The bugs and edgecases can mean that a repartition is done when not needed or,
worse, not done when needed. The incomplete implementation can result in confusing behaviour for
users.

With the introduction of the 'any-key-name' feature, this optimization also becomes redundant, as
users can directly name their key columns as they need.

## What is in scope

- Removal of `WITH(KEY)` from ksqlDB syntax
- Removal of the code with ksqlDB that tracks the key-field.

## What is not in scope

- Future `WITH(KEYS)` style syntax that may be used to control which columns are persisted in the
  key of the Kafka record vs the value.  This is TBD and would be covered in a separate KLIP.

## Value/Return

The feature is not intuitive, it's the source of many a confused user and engineer and is often
found to be the reason some query is not working as expected.

The feature is also incomplete, as it only tracks a single key-field, and buggy, leading to more
confusion and bad query behaviour.feature

The features key benefit of providing an alias for the `ROWKEY` system column will no longer be
required once the naming restriction on the key column is removed.

Removing key fields will simplify the documentation, the code and the mental model users need to
get to grips with to use ksqlDB. It will also open the way for some future release to make use of
similar syntax to control which columns in the schema as persisted in the Kafka record's key vs
value, if that is the direction we choose to go, (i.e. getting with of the `KEY` syntax in the
schema of `CREATE STREAM` statements).

## Public APIS

Removal of support for the `KEY` property in the `WITH` clause of `CREATE TABLE` and `CREATE
STREAM` statements.

## Design

As above.

## Test plan

Our historic testing will ensure existing queries continue to function correctly. All existing
test cases will be updated to name their key column in their schema statement, not in the `WITH`
clause.

## LOEs and Delivery Milestones

Milestone 1: Parser to reject `WITH(KEY)`: LOE: ~2 days
Milestone 2: Removal of key-field code: LOE: ~3 days.

Doc updates: 2-5 days, though this effort can be shared with other syntax altering changes so that
only a single pass across the documentation and examples is needed for all syntax changes.

## Documentation Updates

Documentation and examples in the ksqlDB repo and Confluent examples repo will be updated with the
new syntax.

## Compatibility Implications

Existing queries will continue to run as before. New queries submitted with the old syntax will
be rejected. They will fail with a descriptive error explaining the syntax is no longer supported
and what the replacement is, i.e. they won't fail with a unhelpful parser exception.

## Security Implications

None.
