# KLIP 24 - KEY column semantics in queries

**Author**: @big-andy-coates |
**Release Target**: TBD |
**Status**: In Discussion |
**Discussion**: TBD

**tl;dr:** Persistent queries do not allow the key column in the projection as key columns are
currently _implicitly_ copied across. This KLIP proposes flipping this so that the key column is
_not_ implicitly copied across, and is therefore _required_ in persistent query projections.

## Motivation and background

Below, we contrast the differences between transient and persistent queries and their current key
column handling semantics.

Note: in all the example sql in this KLIP:
 * `EMIT CHANGES` had been removed for brevity.
 * `ROWTIME` had been ignored for brevity.
 * Schemas use `KEY` to denote the column stored in the Kafka record's key.
 * `PRIMARY KEY` is not used for brevity.
 * All input sources use the schema `ID INT KEY, V0 INT, V1 INT`, unless otherwise stated.

Transient push and pull queries only return the columns in their projection. This is inline with
standard SQL. Conversely, persistent queries _implicitly_ copy the key column across, and don't
allow the key column in the projection.

### Simple query

The difference in key column handling can be seen with a simple query:

```sql
SELECT ID, V0, V1 FROM INPUT;
-- resulting columns: ID INT, V0 INT, V1 INT

-- vs --

CREATE TABLE OUTPUT AS SELECT ID, V0, V1 FROM INPUT;
-- fails with error about duplicate 'ID' column.
```

Where as the above transient query works and returns all columns, converting it to a persistent
query causes it to fail with a duplicate column error, because `ID` is _implicitly_ being copied to
the OUTPUT schema, and the `ID` in the projection is creating a value column called `ID`. That's two
columns named `ID` and hence the query fails.

We propose that the above `CREATE TABLE` statement should work, as the projection is equivalent to
`select *`, which works.

### Select star

The differences with a 'select *' projection are more subtle.

For non-join queries, both transient and persistent queries select all the columns in the schema:

```sql
SELECT * FROM INPUT;
-- resulting columns: ID INT, V0 INT, V1 INT

-- vs --

CREATE TABLE OUTPUT AS SELECT * FROM INPUT;
-- resulting schema: ID INT KEY, V0 INT, V1 INT (Same as above)
```

With a join query, a transient query selects all columns from all sources. A persistent query adds
all columns from all sources, but also adds an additional copy of the left key column.

```sql
SELECT * FROM INPUT I1 JOIN INPUT_2 I2 ON I1.ID = I2.ID;
-- resulting columns: I1_ID INT, I1_V0 INT, I1_V1 INT, I2_ID INT, I2_V0 INT, I2_V1 INT

-- vs --

CREATE TABLE OUTPUT AS SELECT * FROM INPUT I1 JOIN INPUT_2 I2 ON I1.ID = I2.ID;
-- resulting schema: ID INT KEY, I1_ID INT, I1_V0 INT, I1_V1 INT, I2_ID INT, I2_V0 INT, I2_V1 INT
```

Note the addition of an additional `ID` column in the case of the persistent query.

We propose that the join should not duplicate the left join column `I1.ID` into both the `ID` key
and `I1_ID` value column.

Only the key column should exist and it should be called either (TBD):
  - `ID`, or
  - `I1_ID`.

Additionally, we may want to consider dropping `I2.ID` from the result, given we know it is equal
to `I1.ID`. This effectively de-duplicates the data and saves on resources. Whether we do this is
TBD.

### Removal of non-standard Aliasing

The [allow any key name](https://github.com/confluentinc/ksql/issues/3536) feature has introduced
the ability to provide an alias within a `PARTITION BY`, `GROUP BY` and `JOIN` clause. These are
non-standard, but required in the current model to allow users to name the key column in the schema.
For example:

```sql
-- without alias:
CREATE TABLE OUTPUT AS SELECT ID FROM INPUT PARTITION BY V0 - V1;
-- resulting schema: KSQL_COL_0 INT KEY, ID;
-- note the system generated column name.

-- with alias
CREATE TABLE OUTPUT AS SELECT ID FROM INPUT PARTITION BY V0 - V1 AS NEW_KEY;
-- resulting schema: NEW_KEY INT KEY, ID;
```

However, the same functionality can be achieved using standard sql if the key column is required
in the projection, for example:

```sql
CREATE TABLE OUTPUT AS SELECT V0 - V1 AS NEW_KEY, ID FROM INPUT PARTITION BY V0 - V1;
-- resulting schema: NEW_KEY INT KEY, ID;
```

### Any key name

Persistent queries failing if the projection contained the key column existed before the work to
[allow any key name](https://github.com/confluentinc/ksql/issues/3536), though this work also
exacerbated the situation, as the key column is no longer always called `ROWKEY`. Instead, users can
pick any name for the key column, and the key column takes on the name of any `PARTITION BY`,
`GROUP BY` or `JOIN ON` on a single column. This causes seemingly correct and common patterns to
fail with duplicate column errors:

```sql
-- Before 'any key name':
CREATE STREAM INPUT (ROWKEY INT KEY, V0 INT, V1 INT) WITH (...);
CREATE TABLE OUTPUT AS SELECT V0, COUNT(*) AS COUNT FROM INPUT GROUP BY V0;
-- resulting schema: ROwKEY INT KEY, V0 INT, COUNT BIGINT
-- Note: ROWKEY & V0 store the same data, which isn't ideal.

-- vs --

-- After 'any key name':
CREATE STREAM INPUT (ID INT KEY, V0 INT, V1 INT) WITH (...);
CREATE TABLE OUTPUT AS SELECT V0, COUNT(*) AS COUNT FROM INPUT GROUP BY V0;
-- fails with duplicate column error on 'V0'.

-- Note: the equivalent query would of failed before the any-key-name work:
CREATE TABLE OUTPUT AS SELECT ROWKEY, COUNT(*) AS COUNT FROM INPUT GROUP BY V0;
-- fails with duplicate column error on 'ROWKEY'.
```

As you can see from above, the common pattern of selecting the group by key and the aggregate fails.
To 'fix' the query the user must remove `V0` from the projection - which is counter intuitive, as
the user wishes this column in the result.

We propose that this query should work, without any modification, and without storing duplicate
data.

### Requiring the key column in a projection in persistent queries

By flipping the semantics so that the key column is not _implicitly_ copied across in persistent
queries we place the burden on the user to _explicitly_ specify it in the projection.

Where as it is possible to have a transient query without the key column in the projection, as a
transient query is simply returning tabular data and has no concept of a key column, persistent
queries do have the concept of a key column, and changing that column is only allowed via an
explicit `PARTITION BY` or `GROUP BY` statement.  Therefore users will _always_ have to specify the
key column in the projection of persistent queries.

For example, the following statements, which previously executed, will now fail:

```sql
CREATE TABLE OUTPUT AS SELECT V0, V1 FROM INPUT;
-- fails as key column not in projection.

CREATE TABLE OUTPUT AS SELECT COUNT(*) FROM INPUT GROUP BY V1;
-- fails as key column not in projection.

CREATE STREAM OUTPUT AS SELECT V0, ID FROM INPUT PARTITION BY V1;
-- fails as key column not in projection.

CREATE TABLE OUTPUT AS SELECT I1.V0, I2.V2 FROM INPUT I1 JOIN INPUT I2 ON I1.ID = I2.ID;
-- fails as key column not in projection.
```

We propose this is acceptable, and preferable to the current model.


### The thorn in the design's side

Hopefully, all of the above seems simple and clear. Now for the tricky and murky bit...

The observant among you may be thinking 'ah, but what if I _want_ to put a copy of the key column
into the value?'. With current syntax you can do:

```sql
-- current syntax:
CREATE TABLE OUTPUT AS SELECT V0, V1, ID AS V2 FROM INPUT;
-- resulting schema: ID INT KEY, V0 INT, V1 INT, V2 INT
```

With the proposed syntax the `ID AS V2` would be treated an aliased key column, resulting in the
schema 'V2 INT KEY, V0 INT, V1 INT'. So, how does the user create an copy of the key in the value,
if that's what they need to do?

Outside of introducing some non-standard keyword into the statement, e.g.

```sql
-- KEY keyword to indicate its a KEY column
CREATE TABLE OUTPUT AS SELECT ID AS V2 KEY, V0, V1 FROM INPUT;
-- resulting schema: V2 INT KEY, V0 INT, V1 INT

-- VALUE keyword to indicate its a VALUE column
CREATE TABLE OUTPUT AS SELECT ID, V0, V1, ID AS V2 VALUE FROM INPUT;
-- resulting schema: ID INT KEY, V0 INT, V1 INT, V2 INT
```

Which are pretty yuck as its moving away from standard SQL.

Or by adding some `WITH(KEY_COLUMNS=?)` style syntax, e.g.

```sql
CREATE TABLE OUTPUT WITH(KEY_COLUMNS=[ID]) AS SELECT ID, V0, V1, ID AS V2 VALUE FROM INPUT;
-- resulting schema: ID INT KEY, V0 INT, V1 INT, V2 INT
```

Which is better, but still a little verbose. Also, it would be nice to not couple this KLIP to any
discussion about 'Should ksqlDB have `KEY` columns in the schema of CSAS statements or list them in
the WITH clause?'

Aside from these, the only ways we can think of that are standard SQL would to treat the first
occurrence of the key column in the projection as naming the key column in the result, and any
subsequent copies to be value columns, (see example below). However, this reliance on the _order_
of columns is less than ideal. Anyone got any better ideas for a short term fix for this edge case?

```sql
CREATE TABLE OUTPUT AS SELECT ID, V0, V1, ID AS V2 FROM INPUT;
-- resulting schema: ID INT KEY, V0 INT, V1 INT, V2 INT


CREATE TABLE OUTPUT AS SELECT ID AS NEW_KEY, V0, V1, ID FROM INPUT;
-- resulting schema: NEW_KEY INT KEY, V0 INT, V1 INT, ID INT
```

## What is in scope

- Removal of implicit copying of key column,
  in favour of requiring key column in projection of persistent queries.
- Removal of non-standard GROUP BY, PARTITION BY and JOIN aliasing syntax,
  in favour of standard aliasing of the key column in the projection.
- removal of duplicate left join column on 'select *' joins.
- TBD: remove of right join column(s) from 'select *' joins.
- TBD: syntax for allowing user to add key column to value schema.

## What is not in scope

- Changes in syntax for changing the key column, e.g. allowing the projection to change the key
  column.  This out of scope and is only potential future work. It should be discussion separately.
- Replacing the use of the `KEY` keyword in CSAS statements with other syntax:
  this is mostly orthogonal to this change.
- everything else.

## Value/Return

Standardizing the key semantics in queries will lower the barrier for entry for users and engineers
alike, and reduce the support burden of explaining the subtleties. It will also simplify the code,
which should result in less bugs.

Removing of the non-standard `GROUP BY`, `PARTITION BY` and `JON ON` aliasing in favour of aliasing
in the projection will improve out standards compliance.

## Public APIS

1. Persistent queries, i.e. those used in `CREATE TABLE AS`, `CREATE STREAM AS` and `INSERT INTO`
statements, will be required to _always_ include their key columns in their projection. An error
will be generated should the projection of a persistent query not include its key column. For
example:

```sql
-- old syntax that worked:
CREATE TABLE OUTPUT AS SELECT COUNT() AS COUNT FROM INPUT GROUP BY V0;
-- will now fail with an error explaining the projection must include the key column `V0`.sql

-- corrected query:
CREATE TABLE OUTPUT AS SELECT V0, COUNT() AS COUNT FROM INPUT GROUP BY V0;
-- resulting schema: V0 INT KEY, COUNT BIGINT
```

2. The, as yet unreleased, non-standard `GROUP BY`, `PARTITION BY` and `JOIN ON` alias syntax will
be removed in favour of using the existing standard-compliant aliasing in the projection. For
example:

```sql
-- 'any key' aliasing syntax that will be dropped:
CREATE TABLE OUTPUT AS SELECT COUNT() AS COUNT FROM INPUT GROUP BY (V0 + V1) AS K;
-- resulting schema: K INT KEY, COUNT BIGINT

-- proposed key column aliasing in projection:
CREATE TABLE OUTPUT AS SELECT (V0 + v1) AS K, COUNT() AS COUNT FROM INPUT GROUP BY V0 + V1;
-- resulting schema: K INT KEY, COUNT BIGINT
```

3. Removal of duplicate left join column on `select *` joins. For example:

```sql
CREATE TABLE OUTPUT AS SELECT * FROM INPUT I1 JOIN INPUT I2 ON I1.ID = I2.ID;
-- current result schema: ID INT KEY, I1_ID INT, I1_V0 INT, I1_V1 INT, I2_ID INT, I2_V0 INT, I2_V1 INT
-- note join key is duplicated in ID, I1_ID and I2_ID columns.

-- proposed result schema either (TBD):
-- a): ID INT KEY, I1_V0 INT, I1_V1 INT, I2_ID INT, I2_V0 INT, I2_V1 INT
-- note join key is duplicated in ID and I2_ID columns, only.
-- b): I1_ID INT KEY, I1_V0 INT, I1_V1 INT, I2_ID INT, I2_V0 INT, I2_V1 INT
-- note join key is duplicated in I1_ID and I2_ID columns, only.
```

4. (Optional: TDB) Removal of duplicate right join column(s) on `select *` joins. For example:

```sql
CREATE TABLE OUTPUT AS SELECT * FROM INPUT I1 JOIN INPUT I2 ON I1.ID = I2.ID;
-- current result schema: ID INT KEY, I1_ID INT, I1_V0 INT, I1_V1 INT, I2_ID INT, I2_V0 INT, I2_V1 INT
-- note join key is duplicated in ID, I1_ID and I2_ID columns.

-- proposed result schema either (TBD):
-- Note: these are the same as for point 3 above, minus the `I2_ID` column
-- a): ID INT KEY, I1_V0 INT, I1_V1 INT, I2_V0 INT, I2_V1 INT
-- note join key is no longer duplicated, it is only in ID column
-- b): I1_ID INT KEY, I1_V0 INT, I1_V1 INT, I2_V0 INT, I2_V1 INT
-- note join key is no longer duplicated, it is only in I1_ID column
```

5. TBD: Syntax for allowing key column to be added as value column

TDB.

## Design

N/A: the change is a simple(ish) syntax change.

## Test plan

All preexisting queries, i.e. those with plans serialized to the command topic, will continue to
work and we have extensive tests covering this.

Existing functional (QTT) tested will be converted to the new syntax with any missing cases added.

As a purely syntactical change, nothing else is required.

## LOEs and Delivery Milestones

This is a small change, deliverable as a single milestone.

## Documentation Updates

Docs and examples in the ksqlDB repo, and any ksqlDB usage in the 'examples' repo, will be checked
to ensure the match the new syntax.

## Compatibility Implications

All preexisting queries, i.e. those with plans serialized to the command topic, will continue to
work and we have extensive tests covering this.

Some existing SQL, if reissued, will fail if a persistent query's projection does not include the
key column. However, a helpful error message will inform the user of the changes they need to make
to resolve this.  Resolution is simple: just add the key column to the projection!

todo: how about existing queries that alias ROWKEY to a value column?

## Security Implications

None.

## Rejected alternatives

As above, but not _requiring_ the key column in the projection. Instead, allow the key column in the
projection and implicitly copy it across if its not there.  This was rejected for two key reasons:
1. Potentially confusing 'magic implicits' - the output contains columns the projection doesn't specify.
2. It over complicates the implementation.
