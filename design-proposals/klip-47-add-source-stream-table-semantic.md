# KLIP 47 - Add Source Stream/Table Semantic 

**Author**: Boyang Chen (@boyang) | 
**Release Target**: TBD | 
**Status**: _In Discussion_ | 
**Discussion**: https://github.com/confluentinc/ksql/pull/7474

**tl;dr:** _Add support for source stream/table. This will give users a read-only view of data that 
are populated by the upstream Kafka topic, making the data ownership more clear._
           
## Motivation and background

Users of KsqlDB have no clear ownership definition around tables or streams they created. In a “normal” database, 
when a user creates a table the DB creates the corresponding files on disk and owns the table. 
User may write data into those tables via `INSERT INTO` statements and/or modify/delete existing data 
via `UPDATE` and `DELETE` statements. However, the table will be not changed by any other means. 

For ksqlDB, the setup is quite differently. Specifically, user could create the database based on 
the topics stored in Kafka as the source of truth. The defined database could be mutable by 
using `INSERT` command to populate data into it, which will then pollute the original data source.
Secondly, when calling `CREATE TABLE`, the actual data is neither materialized nor 
ready for interactive queries to access. To address these gaps, we would like to add:

* Syntax to add read-only stream/table as `CREATE SOURCE STREAM/TABLE` to make the data source as read-only. Any 
insertion command will be rejected, with an example error response as:

```
Error: insertion into source stream/table is not allowed
```   

* In addition, the `CREATE SOURCE TABLE` will materialize the input source topic into a stream 
instance which would generate a RocksDB instance for interactive queries.


## What is in scope
* Add `CREATE SOURCE STREAM/TABLE` syntax to KSQL
* `DESCRIBE ... EXTENDED` should show source streams and tables as read-only  
* Add materialization of the `source table` to make it pull-query accessible
* Make source stream/table immutable from KSQL by rejecting insertion requests
 

## What is not in scope
* We will not add role-based write permission for source stream/table in v1, even though it seems to be useful.

## Design

### Semantic Change
We shall first add optional keyword `SOURCE` into the KSQL codebase, by adding the new syntax to `sqlBase.g4` for:
* createStream
* createStreamAs
* createTable
* createTableAs

Then inside `AstBuilder#visitCreateStream` and `AstBuilder#visitCreateTable`, the built classes for stream/table 
instances would be `CreateStream` and `CreateTable`, which are the only two subclasses of `CreateSource`. It makes sense 
to add the `SOURCE` as a flag into `CreateSource`, and let `AstBuilder` pass the flag into the struct 
for later stage reference of read-only.

In addition, when user calls `DESCRIBE ... EXTENDED`, the read-only attribute shall be displayed for stream/table. 

### Make Source Stream/Table Read-only
We plan to add restriction check into the `DistributingExecutor#throwIfInsertOnReadOnlyTopic` or create a similar function, 
which could verify whether the given stream/table is read-only. If so, the insertion will be rejected.

### Source Table Materialization
In the call `EngineExecutor#execute`, we will check whether given plan has a `queryPlan` before deciding to proceed 
creating a persistent query. To bypass this check, we need to provide a concrete query plan within the input plan. We would 
inject a dummy query plan in `EngineExecutor#plan` phase, when the statement is a CreateTable statement with `SOURCE` tag.

In addition, when user tries to show all the running queries, the materialization query should also be displayed as it takes 
part of the computation resource. This would be a special type of running query without `SELECT` clause.

Here comes a precaution we might be aware. The original `CREATE TABLE` command does not need materialization, so there is 
no enforcement for primary key. However, in order to let pull query run on the materialized state, the store must be keyed so that 
pull query could use the primary key for access.  

To enforce primary key on the `CREATE SOURCE TABLE` command, we have 3 options:
1. `AstBuilder` could verify that at least one `TableElement` contains primary key, otherwise the plan fails.
2. `LogicalPlanner#buildPersistentLogicalPlan` could verify the plan node and see whether it needs materialization 
but has no defined primary key.
3. Create an injector similar to `TopicCreateInjector`, which could be used to validate things through `RequestValidator`

In long-term, we don't want to bloat `AstBuilder` with all the logical constraint checks. In the short-term, the complexity 
of #1 is low, and the expected code change to enforce this constraint should be low, so we would pick #1. 

## Public APIS
* Create source table/stream, which makes the data source read-only:

```roomsql
CREATE SOURCE STREAM stream_name (COL1 INT, COL2 STRING) AS ...
CREATE SOURCE TABLE table_name (ID INT KEY, COL1 INT) AS ...
```

## Test plan
The new semantics will be tested as:

* Unit testing for modular changes
* QTTs with the new source table/stream syntax, including insertion failure and successful materialization

## LOEs and Delivery Milestones
The estimated work time will be spent as follows (to implement and review):
1. Add `SOURCE` syntax for both stream/table takes a week
2. Make changes in Metastore and CreateSource for read-only sources takes a week to review
3. Reject insertion to `SOURCE` table/stream takes a week
4. Display read-only attributes on `DESCRIBE ... EXTENDED` 3 days 
5. Source table materialization takes a week
6. Documentation changes takes 3 days
7. Integration testing takes 2 weeks

There will be 3 milestones, first is the completion of syntax addition (#1), which marks the start 
point of splitting the work between engineers. Second milestone will be the feature complete ($2~5), and 
the last one will be documentation and tests complete.

## Documentation Updates
* Add a new doc page as `docs/developer-guide/ksqldb-reference/source-table.md`
* Add a new doc page as `docs/developer-guide/ksqldb-reference/source-stream.md`

## Compatibility Implications
This is a new feature in CTAS and CSAS, which has no backward compatibility issue. 

## Security Implications
None