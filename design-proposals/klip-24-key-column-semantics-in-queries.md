# KLIP 24 - KEY column semantics in queries

**Author**: @big-andy-coates |
**Release Target**: 0.10.0; 6.0.0 |
**Status**: _Merged_ |
**Discussion**: [Github PR](https://github.com/confluentinc/ksql/pull/5115)

**tl;dr:** Persistent queries do not allow the key column in the projection as key columns are
currently _implicitly_ copied across. This is not intuitive to anyone familiar with SQL. This KLIP
proposes flipping this so that the key column is _not_ implicitly copied across.

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

CREATE TABLE OUTPUT AS
   SELECT ID, V0, V1 FROM INPUT;
-- fails with error about duplicate 'ID' column.
```

Where as the above transient query works and returns all columns, converting it to a persistent
query causes it to fail with a duplicate column error, because `ID` is _implicitly_ being copied to
the OUTPUT schema, and the `ID` in the projection is creating a value column called `ID`. That's two
columns named `ID` and hence the query fails.

We propose that the above `CREATE TABLE` statement should work, as the projection is equivalent to
`select *`, which works.

### Non-join select star

For non-join queries, both transient and persistent queries select all the columns in the schema:

```sql
SELECT * FROM INPUT;
-- resulting columns: ID INT, V0 INT, V1 INT

-- vs --

CREATE TABLE OUTPUT AS
   SELECT * FROM INPUT;
-- resulting schema: ID INT KEY, V0 INT, V1 INT (Same as above)
```

### Joins

The differences with joins is more subtle.

With a join query, a transient query selects all columns from all sources. A persistent query adds
all columns from all sources, and also adds an additional column that stores the result of the join
criteria.

```sql
SELECT * FROM INPUT I1 JOIN INPUT_2 I2 ON I1.ID = I2.ID;
-- resulting columns: I1_ID INT, I1_V0 INT, I1_V1 INT, I2_ID INT, I2_V0 INT, I2_V1 INT

-- vs --

CREATE TABLE OUTPUT AS
   SELECT * FROM INPUT I1 JOIN INPUT_2 I2 ON I1.ID = I2.ID;
-- resulting schema (any key name enabled): ID INT KEY, I1_ID INT, I1_V0 INT, I1_V1 INT, I2_ID INT, I2_V0 INT, I2_V1 INT
-- resulting schema (any key name disabled): ROWKEY INT KEY, I1_ID INT, I1_V0 INT, I1_V1 INT, I2_ID INT, I2_V0 INT, I2_V1 INT
```

Note the addition of an additional `ID` or `ROWKEY` column in the case of the persistent query.

#### Inner and left outer joins on column references

For inner and left outer joins where the left join criteria is a column reference the
additional `ID` column is a duplicate of the `I1.ID` column.

We propose that such joins should not duplicate the left join column `I1.ID` into both the `ID` key
and `I1_ID` value column. Instead, the key column should be named `I1_ID` and no copy should be stored
in the value.

#### Full outer joins and non-column joins

Where the join is a full outer join, or where the left join criteria is not a column reference, the
problem is more nuanced.

First, consider a full outer join on columns from the left and right sources:

```sql
-- full outer join:
CREATE TABLE OUTPUT AS
   SELECT * FROM INPUT I1 OUTER JOIN INPUT_2 I2 ON I1.ID = I2.ID;
```

The data stored in the Kafka message's key will be equal to either the left join column, the right
join column or both, depending on whether only one side matches or both:

|                  | I1.ID | I2.ID | Message Key |
| ---------------- | ----- | ----- | ----------- |
| both sides match | 10    | 10    | 10          |
| left side only   | 10    | null  | 10          |
| right side only  | null  | 10    | 10          |

As you can see, the message key is not equivalent to either of the source columns. This is
problematic.

The same is also true of other join types where no sides within the join criteria are a simple
column reference, a.k.a. non-column joins. For example:


```sql
-- inner join on expression:
CREATE TABLE OUTPUT AS
   SELECT * FROM INPUT I1 JOIN INPUT_2 I2 ON ABS(I1.ID) = ABS(I2.ID);
```

Again, the message key is not equivalent to any column for the sources involved in the join. (Note:
if either side of the join criteria is a simple column reference, then the Kafka message's key is
equivalent to that column, and hence no additional column is synthesised by the join).

This KLIP proposes that the projection should include _all the columns expected in the result_.
Logically, this must include this new key column. Yet, this new key column is an artifact of the
current join implementation, so what should it be called and how should the user include it in the
projection?

The synthesised column is currently named `ROWKEY`. However, the 'any key name' feature removes the
`ROWKEY` system column in favour of user supplied names or system generated ones in the form
`KSQL_COL_x`.

If we are to require users to explicitly include this synthesised column in any projection with
explicit columns, i.e. non select-star projections, then the user must be able to determine the name
and be able to provide their own.

Though not ideal, we propose that in the short term the synthesised column will be given a system
generated name. This name will remain `ROWKEY` unless such a column already exists in the schema,
where a integer will be appended to ensure the name is unique, e.g. `ROWKEY_0`, `ROWKEY_1`, etc.
For example, the above examples that used `*` in their projections could be expanded to the
following explicit column lists:

```sql
-- full outer join:
CREATE TABLE OUTPUT AS
   SELECT ROWKEY, I1.ID, I1.V0, I1.V1, I2.ID, I2.V0, I2.V1
      FROM INPUT I1 OUTER JOIN INPUT_2 I2 ON I1.ID = I2.ID;
-- resulting schema: ROWKEY INT KEY, I1_ID INT, I1_V0 INT, I1_V1 INT, I2_ID INT, I2_V0 INT, I2_V1 INT

-- inner join on expression:
CREATE TABLE OUTPUT AS
   SELECT ROWKEY, I1.ID, I1.V0, I1.V1, I2.ID, I2.V0, I2.V1 INT
      FROM INPUT I1 JOIN INPUT_2 I2 ON ABS(I1.ID) = ABS(I2.ID);
-- resulting schema: ROWKEY INT KEY, I1_ID INT, I1_V0 INT, I1_V1 INT, I2_ID INT, I2_V0 INT, I2_V1 INT
```

Key to this solution, is the ability for users to provide their own name for the synthesised key
column. For example:

```sql
CREATE TABLE OUTPUT AS
   SELECT ROWKEY AS ID, I1.V0, I1.V1, I2.V0, I2.V1 INT
      FROM INPUT I1 OUTER JOIN INPUT_2 I2 ON I1.ID = I2.ID;
-- resulting schema: ID INT KEY, I1_V0 INT, I1_V1 INT, I2_V0 INT, I2_V1 INT
```

Requiring the user to include a synthesised column in the projection is not ideal. However, we
propose this is best short term solution given the current functionality.

The name of the column will always be `ROWKEY` unless a column with that name already exists.
When the synthetic column is missing, the error message will detail the name and its reason for
being.

A more correct implementation might store both sides of the join criteria in the key for all join
types. However, such an approach would require support for multiple key columns and extensive
changes in Streams. See [rejected alternatives](#rejected_alternatives) for more info.

### Removal of non-standard Aliasing

The [allow any key name](https://github.com/confluentinc/ksql/issues/3536) feature has introduced
the ability to provide an alias within a `PARTITION BY`, `GROUP BY` and `JOIN` clause. These are
non-standard, but required in the current model to allow users to name the key column in the schema.
For example:

```sql
-- without alias:
CREATE STREAM OUTPUT AS
   SELECT ID FROM INPUT PARTITION BY V0 - V1;
-- resulting schema: KSQL_COL_0 INT KEY, ID;
-- note the system generated column name.

-- with alias
CREATE STREAM OUTPUT AS
   SELECT ID FROM INPUT PARTITION BY V0 - V1 AS NEW_KEY;
-- resulting schema: NEW_KEY INT KEY, ID;
```

However, the same functionality can be achieved using standard sql if the key column is required
in the projection, for example:

```sql
CREATE STREAM OUTPUT AS
   SELECT V0 - V1 AS NEW_KEY, ID FROM INPUT PARTITION BY V0 - V1;
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

CREATE TABLE OUTPUT AS
   SELECT V0, COUNT(*) AS COUNT FROM INPUT GROUP BY V0;
-- resulting schema: ROWKEY INT KEY, V0 INT, COUNT BIGINT
-- Note: ROWKEY & V0 store the same data, which isn't ideal.

-- vs --

-- After 'any key name':
CREATE STREAM INPUT (ID INT KEY, V0 INT, V1 INT) WITH (...);

CREATE TABLE OUTPUT AS
   SELECT V0, COUNT(*) AS COUNT FROM INPUT GROUP BY V0;
-- fails with duplicate column error on 'V0'.
```

As you can see from above, the common pattern of selecting the group by key and the aggregate fails.
To 'fix' the query the user must remove `V0` from the projection - which is counter intuitive, as
the user wishes this column in the result.

We propose that the last query above should work, without any modification, and without storing
duplicate data.

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
CREATE TABLE OUTPUT AS
   SELECT V0, V1 FROM INPUT;
-- fails as key column not in projection.

CREATE TABLE OUTPUT AS
   SELECT COUNT(*) FROM INPUT GROUP BY V1;
-- fails as key column not in projection.

CREATE STREAM OUTPUT AS
   SELECT V0, ID FROM INPUT PARTITION BY V1;
-- fails as key column not in projection.

CREATE TABLE OUTPUT AS
   SELECT I1.V0, I2.V2 FROM INPUT I1 JOIN INPUT I2 ON I1.ID = I2.ID;
-- fails as key column not in projection.
```

We propose this is acceptable, and preferable to the current model.

There is no reason why ksqlDB should not support creating sources in the future that do not contain
the key column. See the [rejected alternatives](#rejected_alternatives) section for more info.

### Allowing the key column to be copied to the value

Hopefully, all of the above seems simple and clear. Now for one of the more tricky and murky bits...

The observant among you may be thinking 'ah, but what if I _want_ to put a copy of the key column
into the value?'. With current syntax you can do:

```sql
-- current syntax:
CREATE TABLE OUTPUT AS
   SELECT V0, V1, ID AS V2 FROM INPUT;
-- resulting schema: ID INT KEY, V0 INT, V1 INT, V2 INT
```

With the proposed syntax the `ID AS V2` would be treated an aliased key column, resulting in the
schema 'V2 INT KEY, V0 INT, V1 INT'. So, how does the user create an copy of the key in the value,
if that's what they need to do?

We propose introducing an `AS_VALUE` function that can be used to
indicate the key column should be copied as a value column. For example,

```sql
CREATE TABLE OUTPUT AS
   SELECT ID, V0, V1, AS_VALUE(ID) AS V2 FROM INPUT;
-- resulting schema: ID INT KEY, V0 INT, V1 INT, V2 INT
```

### Grouping by multiple expressions.

KsqlDB supports grouping by multiple expressions, for example:

```sql
SELECT V0, ABS(V1), COUNT(*) AS COUNT FROM INPUT GROUP BY V0, ABS(V1);
```

However, it does not _yet_ support multiple key columns. If the above is converted to a persistent
query the key is generated by concatenating the string representation of the grouping expressions.
For example:

```sql
CREATE TABLE OUTPUT AS
   SELECT V0, ABS(V1) AS V1, COUNT(*) AS COUNT FROM INPUT GROUP BY V0, ABS(V1);
-- resulting schema: KSQL_COL_0 STRING KEY, V0 INT, V1 INT, COUNT BIGINT
-- where KSQL_COL_0 contains data in the form V0 + "|+|" + ABS(V1)
```

Even though ksqlDB is currently combining the multiple grouping expressions, we propose that the
projection should still accept the individual columns, and recognise them as key columns. This will
be compatible with the upcoming multiple-key-column support.

However, this posses a problem, as it does not provide a single place where the user can provide an
alias for the system generated `KSQL_COL_0` key column name. Any solution to allow providing an
alias would likely be incompatible with the planned multiple key column support. 

Hence, we propose leaving this edge case unsolved, i.e. users will _not_ be able to provide an alias
for the name of the key column resulting from multiple grouping expressions. This will be resolved
when support for multiple key columns is added.

Alternatively, ksqlDB could support the non-strandard `GROUP BY (a, b) AS c` style aliasing, to allow
users to provide their own name. This support could be removed once multiple key columns are 
supported.

If anyone has any suggestions on how we can support this in a compatible manner, please speak up!

## What is in scope

- Removal of implicit copying of key column,
  in favour of requiring key column in projection of persistent queries.
- Removal of non-standard GROUP BY, PARTITION BY and JOIN aliasing syntax,
  in favour of standard aliasing of the key column in the projection.
- removal of duplicate left join column on 'select *' joins.
- Addition of an `AS_VALUE` function to allow users to copy key column into value columns.
- Exposure of synthetic key columns in some joins, and ability to define an alias for it.

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
CREATE TABLE OUTPUT AS
   SELECT COUNT() AS COUNT FROM INPUT GROUP BY V0;
-- will now fail with an error explaining the projection must include the key column `V0`.sql

-- corrected query:
CREATE TABLE OUTPUT AS
   SELECT V0, COUNT() AS COUNT FROM INPUT GROUP BY V0;
-- resulting schema: V0 INT KEY, COUNT BIGINT
```

2. The, as yet unreleased, non-standard `GROUP BY`, `PARTITION BY` and `JOIN ON` alias syntax will
be removed in favour of using the existing standard-compliant aliasing in the projection. For
example:

```sql
-- 'any key' aliasing syntax that will be dropped:
CREATE TABLE OUTPUT AS
   SELECT COUNT() AS COUNT FROM INPUT GROUP BY V0 AS K;
-- resulting schema: K INT KEY, COUNT BIGINT

-- proposed key column aliasing in projection:
CREATE TABLE OUTPUT AS
   SELECT V0 AS K, COUNT() AS COUNT FROM INPUT GROUP BY V0;
-- resulting schema: K INT KEY, COUNT BIGINT
```

3. Removal of duplicate left join column on `select *` joins. For example:

```sql
CREATE TABLE OUTPUT AS
   SELECT * FROM INPUT I1 JOIN INPUT I2 ON I1.ID = I2.ID;
-- current result schema: ID INT KEY, I1_ID INT, I1_V0 INT, I1_V1 INT, I2_ID INT, I2_V0 INT, I2_V1 INT
-- note join key is duplicated in ID, I1_ID and I2_ID columns.

-- proposed result schema:
-- b): I1_ID INT KEY, I1_V0 INT, I1_V1 INT, I2_ID INT, I2_V0 INT, I2_V1 INT
-- note join key is duplicated in I1_ID and I2_ID columns, only.
```

4. New `AS_VALUE` function to key column to be added as value column:

A new `AS_VALUE` method will be added to allow users to copy the key column into a value column.
For example:

```sql
CREATE TABLE OUTPUT AS
   SELECT ID, V0, V1, AS_VALUE(ID) AS V2 FROM INPUT;
-- resulting schema: ID INT KEY, V0 INT, V1 INT, V2 INT
```

5. Exposure of synthetic key columns in some joins, and ability to define an alias for it.

The synthetic key column created by some joins will be given a system generated name in the form
`ROWKEY[_n]`, where `n` is a positive integer. This will generally be `ROWKEY` unless the
the schemas already include similarly named columns.

Users will be required to explicitly include this synthetic key column in projections and,
optionally, define an alias for it. For example:

```sql
CREATE TABLE OUTPUT AS
   SELECT ROWKEY AS ID, I1.V0, I2.ID FROM I1 FULL OUTER JOIN I2 ON I1.ID = I2.V0;
-- resulting schema: ID INT KEY, V0 INT, ID INT
```

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

## Security Implications

None.

## Rejected alternatives

### Magic key column copying if no key column in persistent query's projection.

Design as above, but not _requiring_ the key column in the projection. Instead, allow the key
column in the projection and implicitly copy it across if its not there.  This was rejected for two
key reasons:
  a. Potentially confusing 'magic implicits' - the output contains columns the projection doesn't
     specify.
  b. It over complicates the implementation.

### Full support for no key column in persistent query's projection.

Design as above, but allowing persistent query projections to not include the key column in the
projection. If not present, the created data source would have no _exposed_ key column.

Given that the created source is actually a materialized view, it seems completely reasonable to
allow users to control the set of columns the view exposes. Any requirement internally for the key,
e.g. to allow updates to be processed correctly, would remain. The key would just not be available
in downstream queries.

This was rejected for this KLIP as it would involve considerably more work. This may be picked up in
a future KLIP.

### Storing all join columns in the key of the result

A more correct solution for handling columns within a join may look to store all join columns in the
Kafka record's key, for example:

```sql
SELECT * FROM INPUT I1 JOIN INPUT_2 I2 ON I1.ID = I2.ID;
-- resulting columns: I1_ID INT, I1_V0 INT, I1_V1 INT, I2_ID INT, I2_V0 INT, I2_V1 INT

CREATE TABLE OUTPUT AS
   SELECT * FROM INPUT I1 JOIN INPUT_2 I2 ON I1.ID = I2.ID;
-- resulting schema: I1_ID INT KEY, I2_ID INT KEY, I1_V0 INT, I1_V1 INT, I2_V0 INT, I2_V1 INT
```

Note that both `I1_ID` and `I2_ID` are marked as key columns. Such an approach may be required if
ksqlDB is to support join criterion other than the current equality.

However, this is rejected as a solution for now for the following reasons:
  a. Such a solution requires ksqlDB to support multiple key columns. It currently does not, and this
     KLIP is part of the work moving towards such support. Hence its a chicken and egg problem.
  b. Such a solution requires Streams to be able to correctly handle the multiple key columns,
     which it currently does not.  This is particularly challenging for outer joins,
     where some key columns may initially be `null` and later populated. Any solution needs to ensure
     correct partitioning and update semantics for such rows.

### Special UDF used to include the synthesised join column

Where a join introduces a synthesised key column the column could be included in the projection
using a special UDF, for example:

```sql
CREATE TABLE OUTPUT AS
   SELECT JOINKEY(I1.ID, I2.ID), I1.V0, I2.V1 FROM I1 FULL OUTER JOIN I2 ON I1.ID = I2.ID;
```

This was rejected as:
  1. it requires introducing a special udf, which would then need to be supported going forward
     even once joins supported storing all key columns in th key.
  2. it's horrible syntax!

#### Data driven naming

The synthesised column would take on a name generated from the join criteria. For example, a join
such as `A JOIN B ON A.ID = B.ID` would result in a key column named `A_ID__B_ID`.

While this is deterministic and offers improved protection against column name clashes than a static
naming strategy, it was rejected as:
  a. it adds additional complexity to the code
  b. it adds additional cognitive load for users, i.e. the need to know the naming strategy and work
     out the name from the criteria.
  c. a change in the join criteria requires a change in the projection.
  d. name clashes are still possible.
  e. generating a name from expressions would be tricky


