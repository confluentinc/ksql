# KLIP 49 - Add Source Stream/Table Semantic 

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

For ksqlDB, the setup is quite differently. Specifically, user could create the table based on 
the topics stored in Kafka as the source of truth. However, since there is no clear ownership, the defined table 
could be mutated by using `INSERT` command to populate data into it. The insertion will pollute the original data source, 
which was supposed to be owned by upstream such as a CDC pipeline that populates the topic.
Secondly, when calling `CREATE TABLE`, the actual data is neither materialized nor ready for interactive queries to access. 
To address these gaps, we would like to add:

* Syntax to add read-only stream/table as `CREATE SOURCE STREAM/TABLE` to make the data source as read-only. Any 
insertion command will be rejected, with an example error response as:

```
Error: insertion into source stream/table is not allowed
```   

* In addition, the `CREATE SOURCE TABLE` will become a persistent query instead of just a metadata operation like 
`CREATE TABLE`, `CREATE STREAM`, or `CREATE SOURCE STREAM`. Source table will materialize the input topic data as 
a RocksDB instance for interactive queries in the KsqlDB servers.

## What is in scope
* Add `CREATE SOURCE STREAM/TABLE` syntax to KSQL
* `DESCRIBE ...` should show source streams and tables as read-only  
* Add materialization of the `source table` to make it pull-query accessible
* Make source stream/table immutable from KSQL by rejecting insertion requests
* Prevent `DROP` command from deleting source stream/table backed up topic
 

## What is not in scope
* We shall not attempt to have data materialization for general `CREATE TABLE` command as default. The reasoning is 
that we lack user feedback around a massive change, and it's preferable to start giving user the option to try out 
materialized table and see if that is a good alternative or not. 
* We will not add role-based write permission for source stream/table in v1, even though it seems to be useful.

## Design

### Semantic Change
We shall first add optional keyword `SOURCE` into the KSQL codebase, by adding the new syntax to `sqlBase.g4` for:
* createStream
* createTable

with the sample syntax as:
```
CREATE (OR REPLACE)? (SOURCE)? TABLE (IF NOT EXISTS)?...
```
Then inside `AstBuilder#visitCreateStream` and `AstBuilder#visitCreateTable`, the built classes for stream/table 
instances would be `CreateStream` and `CreateTable`, which are the only two subclasses of `CreateSource`. It makes sense 
to add the `SOURCE` as a flag into `CreateSource`, and let `AstBuilder` pass the flag into the struct 
for later stage reference of read-only.

Additionally, when user calls `DESCRIBE ... EXTENDED`, the read-only attribute shall be displayed for stream/table. 

### Make Source Stream/Table Read-only
We plan to add restriction check into the `DistributingExecutor#throwIfInsertOnReadOnlyTopic` or create a similar function, 
which could verify whether the given stream/table is read-only. If so, the insertion will be rejected.

Additionally, when calling `DROP` command on a source stream/table, the underlying topic should not be deleted as the source 
stream/table has no ownership of the input data. See this [issue](https://github.com/confluentinc/ksql/issues/3585) for more context. 

### Source Table Materialization
In the call `EngineExecutor#execute`, we will check whether given plan has a `queryPlan` before deciding to proceed 
creating a persistent query. To bypass this check, we need to provide a concrete query plan within the input plan. We would 
inject a dummy query plan in `EngineExecutor#plan` phase, when the statement is a CreateTable statement with `SOURCE` tag.

In addition, when user tries to show all the running queries, the materialization query should also be displayed as it takes 
part of the computation resource. This would be a special type of running query without `SELECT` clause or a query id, as it 
should be only associated with source table, and user could choose to `DROP` the entire table if they don't want to use resource 
to do the materialization.

## Public APIS
* Create source table/stream, which makes the data source read-only:

```roomsql
CREATE SOURCE STREAM stream_name (COL1 INT, COL2 STRING) WITH ...
CREATE SOURCE TABLE table_name (ID INT PRIMARY KEY, COL1 INT) WITH ...
```

## Test plan
The new semantics will be tested as:

* Unit testing for modular changes
* QTTs with the new source table/stream syntax, including insertion failure and successful materialization
* Tests for `DESCRIBE` sources

## LOEs and Delivery Milestones
The estimated work time will be spent as follows (to implement and review):
1. Add `SOURCE` syntax for both stream/table takes a week
2. Make changes in Metastore and CreateSource for read-only sources takes a week to review
3. Reject insertion to `SOURCE` table/stream takes a week
4. Display read-only attributes on `DESCRIBE ... EXTENDED` 3 days 
5. Source table materialization takes a week
6. Add pull query access for source table takes about 3 days
7. Documentation changes takes 3 days
8. Integration testing takes 2 weeks

There will be 3 milestones, first is the completion of syntax addition (#1), which marks the start 
point of splitting the work between engineers. Second milestone will be the feature complete (#2~6), and 
the last one will be documentation and tests complete.

## Documentation Updates
* Add a new doc page as `docs/developer-guide/ksqldb-reference/source-table.md`
* Add a new doc page as `docs/developer-guide/ksqldb-reference/source-stream.md`

## Compatibility Implications
This is a new feature in CT and CS, which has no backward compatibility issue. 

## Security Implications
This is not a security feature, and it is allowed to `CREATE SOURCE TABLE` over an existing topic that is 
being used by other tables, without any affection to the ownership. In the long term, we will have a full 
RBAC-like model for KSQL, which is logistic to create multiple tables on the same topic with different user roles.
The admin could create the table with write access, while a consumer role should be only allowed to create 
read-only tables.