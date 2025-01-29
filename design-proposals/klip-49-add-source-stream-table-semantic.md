# KLIP 49 - Add Source Stream/Table Semantic 

**Author**: Boyang Chen (@boyang) | 
**Release Target**: 0.22.0; 7.1.0 | 
**Status**: _Merged_ | 
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
Secondly, when calling `CREATE TABLE`, the actual data is neither materialized nor ready for pull queries to access. 
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
* Prevent `DROP ... DELETE TOPIC` command from deleting source stream/table backed up topic
 

## What is not in scope
* We shall not attempt to have data materialization for general `CREATE TABLE` command as default, since it could potentially become 
a breaking change for existing `CREATE TABLE` users who assume it to be a light-weight metadata operation. Since we lack user 
feedback around a massive change, and it's preferable to start giving user the option to try out materialized table and 
see if that is a good alternative or not, and make the first version of proposal backward compatible. 
* We will not add role-based write permission for source stream/table in v1, even though it seems to be useful.

## High Level Design

### Semantic Change
We shall first add optional keyword `SOURCE` into the KSQL codebase, by adding the new syntax to `sqlBase.g4` for:
* createStream
* createTable

with the sample syntax as:
```
CREATE (OR REPLACE)? (SOURCE)? TABLE (IF NOT EXISTS)?...
```

### Make Source Stream/Table Read-only
When doing insertions, KSQL would check whether the given stream/table is read-only. If so, the insertion will be rejected.

Additionally, when calling `DROP` command on a source stream/table, currently user could do `DROP STREAM/TABLE 
[table_name] DELETE TOPIC` to clean up the underlying topic backed by the stream/table. This should not be allowed for 
source stream/table as the entity has no ownership of the input data. See this 
[issue](https://github.com/confluentinc/ksql/issues/3585) for more context. 

### Source Table Materialization
The main difference between a source table and a materialized state by a CTAS query is that we do not populate a sink topic 
for source table, as it is not expected to have a downstream connection. This will be addressed when constructing the 
`CREATE SOURCE TABLE` as a special type of persistent query, which would be an implementation detail. 

On the other hand, for the first version of release, we could propagate that `source table` serves as a syntax sugar 
for `create table; create table as select *` in a minimum, so that in the long run users would feel less surprised when 
they start seeing table materialization as default. In the future as we have higher persistent query limit and 
introduce materialized views and complete the ownership model, we can nudge our messaging to focus on the ownership 
side, and perhaps also allow users to opt-out of materialization if they want to reduce footprint.

Another consideration we have is how to expose such type of query to the end users. Generally speaking, any active 
stream runtime should be represented as a query to present to end users. For source table materialization, it is a 
stateful job that comes with overhead, which should definitely be exposed to end users. However, any exposed query 
could be potentially terminated directly by the end user, which comes with a consistency problem with source table 
semantic agreement. If a source table query could be terminated independently by the user, the original source table 
would be dangling and not available for pull query anymore. To address this problem, we have two approaches:
 
 1. The KSQL server rejects the termination of a source table query and informs end user to try deleting the original source table.
 2. The KSQL server will verify the deletion of a query, and try deleting the associated source table with it.
 
Both approaches are valid here to maintain the consistency, but considering when dropping a source table, the active 
materialization query would be terminated, I'm inclined to suggest taking approach #1 here for simplicity, and avoid 
introducing additional code path to have direct metadata affection when we could just make it as a one-way deletion for 
source table.  

For the materialized store in source table, there would also be a backing-up changelog topic. The reasoning is that the input 
topic could potentially be using a different serde than the internal serde being used by KSQL. Having asymmetric serde for 
serialization and deserialization could lead to potentially state store pollution as we do bulk-loading of source topic 
data into the state store. The similar [issue](https://github.com/confluentinc/ksql/issues/5673) has been raised before where 
KSQL has to turn off topology optimization to avoid having state stores backed up by non-internal topics for changelog.   

#### Pull Query Support
One of the motivation for table materialization is to support pull query, but it comes with a need for having the underlying 
table partitioned by the primary key, in order to ensure pull query works. Here the debate was that whether we should enforce 
a repartition of the input topic blindly to guarantee that it is using the primary key as expected for sharding. The current plan 
would be not worrying about the undefined user behavior during the source table primary key construction here, but instead 
just assume they gave us a well-partitioned input topic whose materialization is ready for pull query. This aligns with 
our original expectation of a SOURCE TABLE as well, where KSQL should not have extra mutation or tweak on the original 
data unless specified via CTAS or other query.  

## Public APIS
Create source table/stream, which makes the data source read-only:

```roomsql
CREATE SOURCE STREAM stream_name (COL1 INT, COL2 STRING) WITH ...
CREATE SOURCE TABLE table_name (ID INT PRIMARY KEY, COL1 INT) WITH ...
```

## Test plan
The new semantics will be tested as:

* Unit testing for modular changes
* QTTs with the new source table/stream syntax, including insertion failure, `DROP ... DELETE TOPIC` command integrity
* Tests for successful materialization and pull query
* Tests for query termination restriction
* Tests for `DESCRIBE` sources
* Tests for `LIST QUERIES`

## LOEs and Delivery Milestones
The estimated work time will be spent as follows (to implement and review):
1. Add `SOURCE` syntax for both stream/table takes a week
2. Make changes in Metastore and CreateSource for read-only sources takes a week to review
3. Reject insertion to `SOURCE` table/stream takes a week
3. Avoid `DROP ... DELETE TOPIC` command to delete underlying topics takes 2 days
4. Display read-only attributes on `DESCRIBE ...` 3 days 
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
This is a new feature in CT and CS, which has no backward compatibility issue. Moving forward, we should leave 
the source table query open for evolvement around its data format, which is why we mentioned adding the changelog 
topic instead of relying on the input topic for backup. In the implementation phase, we would also add the source 
table materialization as a new execution step with versioning support, so that we could be handling the upgrade 
smoothly without the concern to write incompatible data format to old states.

## Security Implications
This is not a security feature, and it is allowed to `CREATE SOURCE TABLE` over an existing topic that is 
being used by other tables, without any affection to the ownership. In the long term, we will have a full 
RBAC-like model for KSQL, which is logistic to create multiple tables on the same topic with different user roles.
The admin could create the table with write access, while a consumer role should be only allowed to create 
read-only tables.
