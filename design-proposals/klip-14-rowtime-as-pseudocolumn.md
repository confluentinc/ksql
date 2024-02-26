# KLIP 14 - ROWTIME as Pseudocolumn

**Author**: @big-andy-coates | 
**Release Target**: 0.9.0; 6.0.0 | 
**Status**: _Merged_ |
**Discussion**: https://github.com/confluentinc/ksql/pull/4026

**tl;dr:**
_`ROWTIME` is currently part of a source's schema. `SELECT *` style queries against a source include `ROWTIME`
as a column. As the number of such meta-columns in due to increase, we propose removing `ROWTIME` from a source's
schema and instead having it as a pseudocolumn, akin to Oracle's `ROWNUM`.  

## Motivation and background

Motivated by discussion on [Gihub issue #3734](https://github.com/confluentinc/ksql/issues/3734). 

`ROWTIME`s current status as a column within a source's schema is confusing for users and confusing in the code base.
It is not a column name users can use within `CREATE TABLE` style statements, nor set within a 
`CREATE TABLE AS SELECT` statement. It is not a field within their data. 
It is a implicit column added to represent the event-time the system uses when processing the record.

`ROWTIME` is very similar to the [`ROWNUM` pseudocolumn in Oracle's RDBS](https://docs.oracle.com/cd/B19306_01/server.102/b14200/pseudocolumns009.htm).
In Oracle, a `SELECT * from Foo` on a table would not include `ROWNUM` in the output, but `ROWNUM` can be included if required:

```sql
SELECT ROWNUM, * FROM FOO;
``` 

`ROWTIME` is currently the only _meta-column_ exposed by KSQL. However, there are plans to extend this to include
things such a the source partition, partition offset and message headers.  Adding these using the same pattern 
will result in very wide set of columns. Many of which the user may have no interest in, but can't 
avoid being returned unless they change their query from a `SELECT *` to one listing all the columns in their data.

We therefore propose that `ROWTIME` should be removed from a source's schema in KSQL and made available through
a pseudocolumn.

## What is in scope

* Removing `ROWTIME` from a `DataSource`'s schema, including responses from the server that describe the schema
* Removing `ROWTIME` columns from the result schema of any `SELECT * ` style queries.
* Making `ROWTIME` available in queries as a pseudocolumn. 
* All query types

## What is not in scope

* Use of `ROWTIME` outside of the projection of a query will continue to work as it does today.

## Value/Return

This will open the door to adding more pseudocolumns in the future, without making the rows returned
from `Select *` really wide, even before the user's data columns are displayed.
  
This will help to simplify the code base, which currently struggles with complexity in this area.
It will also clean up a few known bugs and inconsistencies.

## Public APIS

### Queries

Queries will continue to support accessing the `ROWTIME` column outside of the projection as they do today. 
The only difference will be how `ROWTIME` is treated by the select projection.

#### Select star projections

The `*` in a query's projection will no longer include `ROWTIME` when expanded. 
This covers transient and persistent queries.  e.g.

```sql
-- will NOT include ROWTIME in output:
SELECT * FROM X [EMIT CHANGES];
CREATE STREAM AS SELECT * FROM X;
SELECT * FROM X JOIN Y ON X.ID = Y.ID;
CREATE TABLE AS SELECT X.*, Y.a FROM X JOIN Y ON X.ID = Y.ID;
```

Users wanting `ROWTIME` in the output of transient queries will need to explicitly include it:

```sql
-- will include ROWTIME in output:
SELECT ROWTIME, * FROM X [EMIT CHANGES];
SELECT X.ROWTIME, Y.ROWTIME, * FROM X JOIN Y ON X.ID = Y.ID;
```

#### Explicit ROWTIME in projection

##### Transient queries

Any transient query that explicitly uses `ROWTIME` in the projection will continue 
to work as before, e.g. the statement below will have an output that includes `ROWTIME`:

```sql
-- will include ROWTIME in output:
SELECT A, B, ROWTIME, C FROM X;
```    
 
##### Persistent queries

Persistent queries will continue to work pretty much as they do today. 
This is because a `select *` style operation on a persistent query already 
results in another stream or table that contains a `ROWTIME` (pseudo)column.
So as far as the _public_ api goes, nothing has changed.

The only change to the public API is really just an associated bug fix. 
At present, persistent queries explicitly remove any `ROWTIME` column
from the output schema, e.g. the `ROWTIME` column is silently dropped in the 
example statements below:

```sql
CREATE TABLE T AS SELECT ROWTIME, COL1, COL2 FROM I;
CREATE TABLE T AS SELECT X.ROWTIME AS ROWTIME, COL1, COL2 FROM X JOIN Y ON X.ID = Y.ID;
```

It is proposed that KSQL should reject such statements, as they invalid as 
ROWTIME will continue to be a reserved column name. 

### Other API calls

While queries are the brunt of the change, other areas of KSQL's API also currently 
expose `ROWTIME` as a column within the schema, e.g. `DESCRIBE` on a source or query.
Calls describing sources will no longer include `ROWTIME`. Calls describing queries
will use the same logic as above, i.e. `ROWTIME` will only be valid for transient 
queries and will only be included if explicitly requested. 

## Design

The main design change is to remove the concept of meta columns from `LogicalSchema`.
`ROWTIME` will no longer be an implicit meta-column.

`ROWTIME` will no longer be copied into the output's value schema if a query has a 
`SELECT *` projection.

If the user _explicitly_ requests `ROWTIME` in the projection of a transient query, then 
a `ROWTIME` _value_ column will be added to the output schema.

If the user _explicitly_ requests `ROWTIME` in the projection of a persistent query, then
the statement will be rejected if no alias is provided, as `ROWTIME` is a reserved column name,
or created as a _value_ column if a valid alias is provided.

Where the value of `ROWTIME` is required by expressions outside of the query projection, e.g.
in the WHERE clause, `ROWTIME` will be made available as a _value_ column in the intermediate 
schema used within the Kafak Streams topology, as it is today, and will have a 'final select' 
applied to select only the columns the statements projection require. 

## Test plan

The current set of QTT tests will be sufficient to test this change in functionality.

## Documentation Updates

Documentation will be updated to ensure `ROWTIME` is described as a pseudocolumn and any examples 
updated to reflect that `ROWTIME` may no longer be being returned by some queries.

However, in general, most documentation that mentions `ROWTIME` will still be valid.

# Compatibility Implications

It's a breaking change to no longer return ROWTIME from push and pull `SELECT *` queries. 

## Performance Implications

 `ROWTIME` will now only be part of the intermediate / internal schema when it is required.

For queries that still require `ROWTIME` there should be no performance implications.

For queries that no longer require `ROWTIME` there will be both a space saving in any state stores
and associated saving in CPU cycles from not needing to serialize / deserialize `ROWTIME`, and a 
general CPI cycles saving from not having to add / remove / copy / process `ROWTIME`, 
though this may be negligible.

## Security Implications

None.

## Rejected alternatives

### ROWTIME as a system function

Rejected as it won't work in joins, where the `ROWTIME` needs to be scoped to a specific source within the join.
