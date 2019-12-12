# KLIP 17 - Remove 'INSERT INTO' in favour of SQL Union

**Author**: @big-andy-coates | 
**Release Target**: TBD | 
**Status**: In Discussion | 
**Discussion**: TBD

**tl;dr:** 
`INSERT INTO` statements allow users to create persistent queries that insert into existing streams.
However, this is at odds with the target KSQL stream/table model. Switching to SQL's `UNION ALL` 
operator will allow users to create persistent queries that merge multiple source streams into a 
new stream. While this may not, in its self, seem to offer much additional functionality to users, 
it will unlock future enhancements and help clarify user's mental model of KSQL.

## Motivation and background

`INSERT INTO` statements were added to allow many source streams to be merged into a single sink stream.
Where as a `JOIN` merges the columns from two sources into a row containing columns from both sources, 
`INSERT INTO` looks to merge at the row level: it can be used to create an output stream that contains all 
rows of its input streams merged interleaved. For example, consider a system where there are different 
streams holding orders being placed from different store types, e.g. a stream of `web_orders` and another
of physical `store_orders`. `INSERT INTO` allowed users to merge these two streams of orders together: 

```sql
CREATE STREAM all_orders AS SELECT * FROM store_orders;
INSERT INTO all_orders SELECT * FROM web_orders; 
```

Resulting in a `all_orders` stream that has a schema that matches both `SELECT`s, and contains all 
rows from both streams.

However, this approach has several drawbacks.

1. Each additional stream that needs merging into `all_orders` requires a new SQL statement
and underlying KS topology.
1. The is no conceptual reason that the first statement should be any different from any other
statement inserting into `all_orders`, yet the first must be a `CSAS`, while the latter `INSERT INTO`.
1. It makes it more complicated for a user to drop `all_orders` as they must first terminate all the queries
that right to it, which may be many. 
1. It meant there was no longer a (1 - 0..1) relationship between a data source and a persistent query.
1. It added complexity to KSQL, which now needed to check if there were any `INSERT INTO` queries writing
to a stream before that stream could be dropped.
1. It reduced our ability to optimise the topologies and state stores used to build `all_orders`: 
e.g. if each SELECT writing to `all_orders` was join with some reference table, then each KS topology 
would have a duplicate state-store holding the materialized reference table. 

The first few drawbacks directly effect our users, making the conceptual model harder to understand 
and operating KSQL more tiresome, and potentially hurt adoption.  

The later drawbacks are stopping other improvements to KSQL. Including, though not limited to:

* [KLIP-18](klip-18-distributed-metastore .md): Distributed Metastore: which requires the (1 - 0..1) 
relationship between data source and persistent query that this KLIP will restore.
* [KLIP-19](klip-19-materialize-views.md): Introduce Materialized Views: which looks to clarify the 
semantic difference between a view and a table/stream and their associated semantics. 
This KLIP is related because it removes `INSERT INTO` which inserts into an existing stream.
* [KLIP-20](klip-20_remove_terminate.md): Remove `TERMINATE` statement: which requires the (1 - 0..1)
relationship between data source and persistent query that this KLIP will restore.
* [KLIP-21](klip-21_correct_insert_values_semantics.md): Correct 'INSERT VALUES' semantics: which 
depends on [KLIP-19](klip-19-materialize-views.md).

We propose removing `INSERT INTO` functionality and replacing it with support for the standard SQL 
`UNION ALL` operator. 

The standard SQL `UNION` operator is used to combine the result-set of two or more `SELECT` statement, 
with certain limitations:

* Each `SELECT` statement within the `UNION` must have the same number of columns
* The columns must also have similar data types
* The columns in each SELECT statement must also be in the same order

The `UNION` operator selects only distinct rows by default, where as `UNION ALL` allows duplicates.
We are looking to support _merging_ multiple streams, which, aside from the impracticalities of de-duppling, 
actually requires that duplicates are included. Hence this KLIP proposes the introduction of `UNION ALL`, 
not `UNION`.

UNION ALL Syntax:

```sql
SELECT column_name(s) FROM stream1
UNION ALL
SELECT column_name(s) FROM stream2;
```

Additional sources can be merged by adding additional `UNION ALL` operators:

```sql
SELECT column_name(s) FROM stream1
UNION ALL
SELECT column_name(s) FROM stream2
UNION ALL
SELECT column_name(s) FROM stream3;
```

Note: The column names in the result are equal to the column names in the first `SELECT` statement in the union.

Switching to `UNION ALL` operator will enable users to merge multiple streams together without the drawbacks
of `INSERT INTO`:

* It allows two or more streams to be merge together in a single statement,
* which is a simple conceptual model for users.
* Under the hood it will use a single KS topology,
* meaning only a single copy of reference table state-stores
* and only a single query to terminate before the sink stream can be dropped, (or non at all with 
[KLIP-20](klip-20_remove_terminate.md)).

## What is in scope

* Removal of `INSERT INTO` statements from the syntax and code. (Breaking change).
* Introduction of `UNION ALL` support to merge multiple streams.
* Arbitrary `SELECT` statements in the union, as long as they result in a stream, not a table.
* Support for unions in CSAS and transient push queries.

## What is not in scope

* Introduction of `UNION` operator: which removes duplicates from the result set.
We may choose to introduce this later, but is not required in any MVP.
* Introduction of `UNION ALL` support on tables. though it may not be much work to add this later. 
Support for tables is not required to remove `INSERT INTO` and unblock the other KLIPs, 
hence it has been de-scoped.
* And changes to `INSERT VALUES` functionality: this KLIP does not address any issues with `INSERT VALUES`. 
These will be covered by [KLIP-21](klip-21_correct_insert_values_semantics.md).
* Type coercion: the SQL standard only requires the matching columns from each `SELECT` in the union
to have 'similar data types'. Such type coercion is not required for an MVP and may be added later. 

## Value/Return

This KLIP is mainly about enabling other improvements, (e.g. KLIP-18 through 21), though it will also
simplify the KSQL conceptual model, reducing the cognitive load on users. 

## Public APIS

* Remove of `INSERT INTO` support from the supported SQL syntax.
* Add `UNION ALL` support for streams.

## Design

See [UNION docs on w3schools](https://www.w3schools.com/sql/sql_union.asp) for examples of how `UNION ALL` works.

Under the hood this PR will require support for sub-queries to be added to our query planning pipeline.
Each `SELECT` in the union will be added to the logical plan as a sub-query, and translated into a suitable
physical plan, and ultimately building built into a single KS topology.

The CLI will be updated to detect `INSERT INTO` statements and inform the user they are no longer supported, 
and what to do instead, linking to more documentation. This can be done through a general pattern that can 
be used for other removed statement types.

## Test plan

Suitable `QueryTranslationTest` cases will be added to cover the use of `UNION ALL`. 
At a minimum this should replicate the QTT tests that exist for `INSERT INTO`.

If [KLIP-6: Execution Plans](klip-6-execution-plans.md) has not been merged then any existing 
`INSERT ALL` statements in the command topic will result in an error.  If it has been merged, 
the query may execute, which would be bad, unless we add code to explicitly disable this. 
Hence testing in this area is crucial. 

## Documentation Updates

The removal of `INSERT INTO` will obviously necessitate many areas of documentation being updated. 
Luckily `INSERT INTO` is not heavily used in demos or examples.

A new section will be added to the syntax reference to cover the use of `UNION ALL`. Not much beyond 
that would be needed in our docs as this is a standard SQL operator, and hence well documented. 

The release notes will include details of how to migrate `INSERT INTO` statements to use `UNION ALL`.

# Compatibility Implications

Removal of `INSERT ALL` is clearly a breaking change. However, we _must_ remove it if we are to unlock
the other planned work. 

It is believed the proposed `UNION ALL` operator will be able to replicate all functionality currently 
possible with `INSERT ALL`, with the exception that you will no longer be able to run additional statements
later to add to the set of sources being merged. For example, at the moment you can choose to run additional
`INSERT INTO` statements at any time to include another source into the merge, where as with unions we'll
need support for updating the running query, which will come in time.

There is no plan to provide automated migration tooling or functionality as the ROI would be extremely low.

The main compatibility implication is existing `INSERT INTO` statements in the command topic. There is 
no easy way to migrate these to the new union syntax. Our only option is to make this breaking change 
clear in the release notes and to ignore and log such statement, (we'll need custom code to detect them).

## Performance Implications

Combining multiple `INSERT INTO` and the initial `CSAS` statements into a single topology will result
in an instantaneous increase in performance and decrease in resource utilisation and will unlock our 
ability to do future optimisations that would simply not be possible across the current multiple-topology model.

## Security Implications

None.