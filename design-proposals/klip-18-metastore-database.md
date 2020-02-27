# KLIP 18 - Metastore Database

**Author**: @big-andy-coates | 
**Release Target**: TBD | 
**Status**: In Discussion | 
**Discussion**: TBD

**tl;dr:**
The current command topic needs infinite retention and isn't key compacted, meaning its growth is
unbounded. It also isn't well suited to support future ALTER TABLE or query upgrade functionality.
We propose replacing the command topic with a metastore database, containing tables, backed by
key-compacted change-logs, to track each type of entity.

## Motivation and background

The current command topic is used by the rest endpoint to pass the DDL commands it receives to the
ksql engine to execute, in a defined order. The command topic is also used on a server restart to
rebuild the state of the metastore and the list of persistent queries that should be restarted.

As the command topic is not key compacted it grows over time. It contains the full history of DDL
commands sent to the server, including previously created and dropped sources and types. This means
restarts take longer and longer as the command topic grows, unbounded. It also means ksql needs to
support all the different versions of the command topic message format. With a key compacted topic
it would be possible to replace existing messages with updated versions. This would allow ksqlDB
to upgrade old message formats and hence to age out older message formats over time.

There is complex logic used to rebuild the metastore state and determine the list of persistent
queries to restart and custom types to register. This is done by replaying all statements within the
command topic. This logic has been plagued with bugs and unexpected side effects, and its correct
operation relies on all previously committed commands re-executing without error. Any error when
restarting can effect the final state of the metastore. With each node building its own metastore,
it is possible for different nodes to encounter different errors and hence end up in different,
inconsistent states.

To be able to support ALTER TABLE or ALTER TYPE statements, i.e. statements that alter existing
entities, KSQL needs to be able apply updates in a consistent manner in the presence of concurrent
modification requests. A single-writer per-key pattern ensures concurrent requests are applied
sequentially. Key-compacted change-logs and Consumer Groups provide a standard, fault-tolerant,
pattern for determining the owner of a specific key within a table, which natively provides the
start of a single-writer pattern.

Headless mode currently suffers from potentially undefined behaviour should the server be restarted
with either changes in the sql file, or changes to schemas stored in the Schema Registry. Enhancing
headless mode to maintain a metastore database would allow ksqlDB to detect, and fail on,
incompatible changes to the sql file and to be independent of schema changes.

## What is in scope

* Self-contained and internally consistent matastore database.
* Fault tolerant metastore tables, backed by key-compacted change-log topics.
* Metastore state rebuilds that are decoupled from statement execution failures.
* Tables with a single writer per key pattern, ensuring correct behaviour in the presence of
concurrent modifications.
* metastore tables utilized by interactive and headless mode, further unifying operational modes.
* Headless mode that no longer suffers from undefined behaviour if there are changes to schemas or
to the sql file.
* (optional) Migration logic required to upgrade a ksql cluster from a command topic to metastore
tables.

## What is not in scope

* Removing `INSERT INTO`.  This is prerequisite of the proposed design. It is covered by
[KLIP-17: Sql Union](#4125).
* Removing `TERMINATE`. This is prerequisite of the proposed design. It is covered by
[KLIP-20: remove TERMINATE](https://github.com/confluentinc/ksql/pull/4126).
* Exposing the metastore tables via SQL. Allowing users to query the system tables using standard
sql is very powerful. However, this will be covered by a later KLIP.
* ALTER TABLE and ALTER TYPE support: this KLIP is a building block towards this work. However, the
actual support will be covered by later KLIP(s).
* Automation to migrate older DDL message formats to newer ones: again, this KLIP is the foundation
of such work, but the functionality itself would be covered by other KLIP(s).

## Value/Return

This KLIP sees ksqlDB mature into having a self-contained, internally consistent metastore database.
This provides the foundation for solving known issues and adding new improvements and language
features. For example, allowing ksqlDB to age-out old metastore table formats, or the addition of
ALTER TABLE/STREAM/TYPE functionality. It also addresses the issues and limitations with the current
command topic implementation, for example: complex restart logic, lack of key compaction, etc.

## Public APIS

No public API changes.

## Design

### High level

The current write path for DDL statements can be seen as:

![alt text](ksql-18-images/current-ddl-write-flow.png "Current DDL write flow")

Incoming requests are received by the rest server and persisted to the command topic. All ksql nodes
in the cluster consume this data and apply the series of statements to the engine. Where the DDL is
creating a materialized view the engine attempts to start the query to keep the view up to date.
Finally, it updates the local metastore, which stores its data sources in an in-memory map.

A similar write path exists for custom-types.

We propose replacing the command topic with a metastore database, backed by key-compacted
change-log topics. The metastore database will contain tables for each type the store is
responsible for. Currently this is the data sources and custom types. These tables work along the
same lines as KTables: each table is backed by a changelog topic in Kafka and each KSQL node is
responsible for loading and managing its share of table-partitions, i.e. the database is sharded.

Different DDL statements can be mapped to different operation on one of these tables:

| DDL statement  |  Metastore operations  |
| --------------:| ---------------------- |
| CREATE [TABLE / STREAM] (AS SELECT)? | Insert into `UserDataSources` table |
| DELETE [TABLE / STREAM] | Delete from `UserDataSources` table |
| ALTER [TABLE | STREAM] | (Future functionality) In-place update of `UserDataSources` row |
| CREATE TYPE | Insert into `UserTypes` table |
| DELETE TYPE | Delete from `UserTypes` table |
| ALTER TYPE | (Future functionality) In-place update of `UserTypes` row |

The primary key of each table is the name of the type, i.e. the data source or custom type.

Data sources created by C*AS statements will store the query plan as part of the row within the
`UserDataSoures` table.

When a KSQL server receives a DDL statement, the engine will perform the appropriate operation on
one of the metastore's tables. Internally, the metastore will route the request to the node
that is responsible for the partition of the table the key of the row belongs in, (in a similar way
to IQ in Streams), and that node will validate the request. If the update is valid it will update
its internal state, writing-through to the backing changelog topic, returning a success code. If the
update is invalid, an error code and message are returned to the originating node, and ultimately
back to the user.

As well as the local shard(s) of the distributed metastore db, each metastore will also maintain
a complete _materialized view_ of the tables by consuming their changelog topics.  These _views_ are
read-only.  The rest-server will use these views to serve `SHOW [STREAMS|TABLES|TYPES]` statements.
Updates to the views will result in a callback to the engine, which may choose to react to the
change. For example, a CSAS statement will result in a insert into the `UserDataSources` table, with
an attached query plan. The insert will generate a callback to the engine, which will execute the
plan, i.e. start the persistent query. If the user were to then drop the stream, this would result
in the source's row being deleted from the `UserDataSources` table, and the engine would stop the
persistent query when the callback was invoked.

![alt text](ksql-18-images/proposed-ddl-write-flow.png "Proposed DDL write flow")

With this design, the engine executes the DDL statements by making the appropriate changes to the
metastore tables. As these changes are replicated out to each ksqlDB node it will take any
appropriate action, i.e. start/stop persistent queries, etc.

Server restarts are a simple case of loading the key-compacted topics into the metastore tables
and then starting all the persistent queries necessary to keep materialized views updated. These
queries can be started in any order.

### Data Model

Each table within the metastore will have an associated schema, with data stored in a in-memory
key-value store.

The recent work to redesign the command topic messages to define DDL and query plans can be
leveraged to define the data model of the different entity tables, i.e. the JSON schema introduced
as part of this work can be split and enhanced to define the schemas of each table.

#### `UserDataSources`

KSQL does not currently support having a stream and a table registered with the same name. This
can be modeled by storing both in a the same table, with the name as the primary key.

| Column | Type | Description |
|--------|------|-------------|
| Name (Primary Key) | String | The name of the data source |
| Type   | DataSourceType emum | Stream or table? |
| Sql | String | The SQL statement used to create the source |
| Schema | LogicalSchema | The logical schema of the data source |
| Topic  | String | The Kafka topic the data is stored in |
| QueryPlan (NULLABLE) | String | An optional query plan, which is present if the source is a materialized view |
| TimestampColumn (NULLABLE) | String | The column in the schema that stores the event timestamp |
| Serde Options Columns | Boolean | A set of columns (or a bit field?) used to track serde options |

#### `UserTypes`

| Column | Type | Description |
|--------|------|-------------|
| Name (Primary Key) | String | The name of the type |
| SqlType | String | the type definition |

### Dependencies

The above design relies on, or at least is simplified by, a few other KLIPs:

#### [KLIP 17 - SQL Union](#4125)

The design relies on the fact that there is a one-to-one mapping between DDL statement and
data-source, and between data-source and persistent query.

The current `INSERT INTO` does not fit this one-to-one relationship as the DDL statement does not
create a data-source and creates an additional query associated with an existing data-source.

It would potentially be possible to support `INSERT INTO` with the proposed design by allowing
multiple query plans to be attached to existing data sources. However, this would require the
initial implementation to support `UPDATE TABLE` semantics on the metastore tables themselves.
Without `INSERT INTO` the `UPDATE TABLE` functionality is only required once we start to use this
new design to support `ALTER` functionality on user `STREAM`s, `TABLE`s or `TYPE`s.

#### [KLIP 20 - remove TERMINATE](#4126)

`TERMINATE` stops a running persistent query that is keeping a materialized view (C*AS) up to date.
It isn't really a DDL statement and does not fit into the data model of the proposed design.

It would be possible to support `TERMINATE` by setting a flag on a data source's entry in the
`UserDataSources` table.  However, like `INSERT INTO`, this would require `UPDATE TABLE` semantics
on the metastore's tables, which can otherwise be avoided in the initial implementation.

### Low level

#### DDL request routing

To enable DDL requests to be routed to the appropriate node the metastore database will overload
the Kafka Consumer Group protocol to share ownership information in the same way that Kafka Streams
does for interactive queries. This will allow each node to know the current owner for any key within
one of the metastore tables.

During changes to the membership of the group the owner of a partition may be unknown or change.

Any short period of unknown ownership will be handled internally by ksqlDB, e.g. via a retry
mechanism. If the ownership continues to be unknown the DDL statement will fail.

If ownership moves, such that a node receives a request forwarded by another node, but finds it is
no longer the owner of the partition, then the node will return an error code to the initial node.
The initial node will then forward the request to the new owning node.

#### Single writer tables

To ensure correct operation the design forwards requests to mutate rows within metastore tables to
the node that owns that data. This is to ensure only a single application is writing any specific
entry in a metastore table at any one time. Internally, each KSQL node will also need to ensure only
a single thread is updating a specific entry at any one time.

Additionally, as the metastore tables are internally using Kafka's consumer groups to determine
shard/partition ownership, there is the potentially that the owning node has the partition revoked
while processing a request.  Without suitable synchronisation, the revoked node could still produce
data to the table's change-log. This could cause data inconsistencies and corruption, so must be
avoided.

We propose using Kafka transactions to ensure only a single node can write to the table partition
at any one time. The table-partition owner will mutate the table state within a transaction, where
the `transaction.id` is set to a combination of the cluster's service Id, the name of the changelog
topic, and the partition. Should ownership of the partition change, any in-flight change will fail
as if the new owner has initialized, causing the broker to increment the transaction id's epoch.

This design will give us a guaranteed single-writer per-partition table. This same pattern is useful
for more than just the metastore database. It could also form the basis of a mutable source table,
see [Mutable tables](#mutable-tables) below for more info.

#### Referential integrity

Currently, ensuring the referential integrity of the metastore is handled by the engine. We propose
moving these checks within the metastore itself: the metastore should not allow any operation that
would break the referential integrity of itself.

Single writer tables are not, by themselves, enough to ensure referential integrity within the
metastore database.

##### `UserDataSources`
The data-sources within the metastore often form a DAG with downstream sources being built from
upstream sources. To ensure referential integrity, a source should not be removed if another source
is dependent on it. Likewise, no new source should be added within unknown sources.

To be able to enforce referential integrity the node applying the update to the `UserDataSources`
table must have a complete view of the table. This is needed:

* On insert: to ensure all upstream data-sources are known.
* On delete: to ensure there are no downstream data-sources.

Hence we propose the `UserDataSources` should only have a single partition, meaning the owner of the
partition will hold the complete table.

The current metastore ensures referential integrity by tracking the set of queries that write too,
and the set that read from, a data-source. In the new design, the data persisted to the
`UserDataSources` table will include any query plan, which defines the set of upstream data-sources.
Each row in the table will also track the set of downstream data-sources. The downstream
data-sources will only be held in memory.

Adding a new derived data-source, i.e. via a C*AS statement, will both insert a new entry into
the `UserDataSources` table _and_ update the referential integrity tracking data of each of the
new data-sources upstream sources.

Removing an existing data-source, i.e. via a DROP statement, will both remove the entry from the
`UserDataSources` table _and_ update the referential integrity tracking data of each of the
removed data-sources upstream sources.

Updates are only made in-memory once the data has been committed to the Kafka changelog.

##### `UserTypes`

Custom types do not currently reference one another. Hence the custom types table could use multiple
partitions.  However, we propose only a single partition, as data volumes are low and this will
allow types to reference one another later, if we desire.

#### Fault tolerance

Where ksqlDB is clustered, the design will provide a fault tolerant metastore database. Initially,
we can re-load the metastore table-partitions when a partition moves between nodes, i.e. due
to group membership changing.  This will mean there is a short period where DDL statements will run
slower and may even be rejected. However, this will not effect queries: all persistent queries, pull
and push queries will contain to work.

In the future, it should be possible to use a concept similar to the Kafka Streams _hot standby_
pattern to improve availability. This should be fairly trivial given each node already has a full
copy of each tables data in its _local view_ already.

#### Changelog data format

We propose using JSON, validated using a JSON schema, as the format for the metatore's changelog
topics. This leverages the awesome work @rodesai to define a JSON schema for query plans.

#### Headless mode

In headless mode the metastore database will be built from the statements in the sql file the node
is started with. Like interactive mode, the metastore database will be shared across nodes and use
the same single-writer pattern to ensure consistency.

This changes the role of the sql file headless mode is started with from defining the complete set
of sql the node should run with, to only defining the delta, i.e. the list of statements to apply
after any previously applied statements.  Users can make use of `IF NOT EXIST` predicates to ensure
subsequent restarts do not fail due to entities already existing.

Any statements that use schema inference to load schemas from the Schema Registry will having the
column set baked into them the first time they are executed.  As the sources will be persisted in
the metastore database they will be independent of changes in the Schema Registry.

#### LIST operations

The ksqlDB server API supports `LIST` operations on data-sources and custom types. We propose that
these will be serviced from the local view that each node will maintain.   This will mean all/any
node can serve responses to such `LIST` requests.

Serving `LIST` operations from a local view does does introduce a race condition: a `LIST TABLES`
issued immediately after a `CREATE TABLE` statement may not include the new table in the response
if the view has not yet consumed the data.

The existing API gets around this issue by returning a sequence id in the response from DDL
statements. This sequence id can be passed in with subsequent requests to ensure responses include
the result of the previous DDL statement.

Providing the same sequence-id from the metastore database would be more complex as there is no
single topic to use to generate the sequence id from. (The sequence-id is just the offset into the
command topic that the command was written at).

It would be possible to maintain this functionality if all metastore tables shared a single
topic-partition, e.g. the key could include the table identifier.

TBD: is there an alternative pattern we can leverage here? Do we think read-your-own-writes
consistency is important enough to switch the design to use a single shared topic-partition for
all data?

### Future work

#### ALTER TABLE | STREAM | TYPE

As discussed, this work is a building block towards supporting the `ALTER` family of statements.
Each member can be implemented in a similar manner.

For example, ksqlDB could support adding a column to an existing STREAM, TABLE or TYPE.

```sql
ALTER TABLE table_name
ADD column_name datatype;
```

Note, this statement requires the existing table definition. Hence the implementation needs to read
the current table definition, apply the DDL statement, and write
the new table definition, including updating any query plans. All in an atomic manner.

This could be implemented by the request being forwards to the node that owned the STREAM, TABLE or
TYPE's row in the appropriate metastore table.  That node can apply the read-update-write to the
types data. The result will then be replicated to all nodes, which can then update any running
queries.

#### ALTER MATERIALIZED VIEW

A large shortcomings highlighted in [KLIP-17: Sql Union](#4125) was that an SQL Union does not
provide the same flexibility as the current `INSERT INTO` functionality. The former requires the
user to define the full set of sources to query when creating the union, where as the later supports
additional persistent queries being started later to add a new source.

With this design implemented ksqlDB can add `ALTER MATERIALIZED VIEW` support. Initially, this
could be limited to adding a new source to an existing SQL union, closing the functional gap with
`INSERT INTO`. Support for additional changes to existing views can be added later.

#### Mutable tables

Currently tables in ksql are a bit broken, as they allow updates from multiple sources, i.e. Kafka
via changes to the changelog and KSQL via `INSERT VALUES` statements.
[KLIP-19](https://github.com/confluentinc/ksql/pull/4177) goes into more detail on this. It proposes
separating _read-only_ materialized views from mutable tables, with the mutable tables being future
work.

The single-writer mutable tables required by the metatore database could provide the patterns and/or
code for building the mutable tables discussed in KLIP-19.

ksqlDB could allow mutation of these tables with `INSERT VALUES` and `UPDATE TABLE` style
operations. Changes to a table would be persisted to its changelog topic. Downstream sources can use
the mutable table as a source.

## Test plan

Existing functional tests should be sufficient to test this functionality at the high-level, e.g.
there are tests that ensure metastore state is correct after a restart.  These tests will be
modified and enhanced as needed.  Obviously, lots of new unit / functional tests around new
functionality.

In particular, we propose adding tests around the mutable-table, and and the `UserDataSources`'s
tables referential integrity checking, to ensure the implementation is bullet proof. The tests will
use multiple threads and consumer group rebalancing events.

## Documentation Updates

Limited.

Any documentation that references the current command topic will be updated.

If the migration tool is implemented, it will require documentation.

Existing blog posts, etc, will not be changed.

# Compatibility Implications

We can optionally provide a way to migrate existing command-topic based clusters over to the new
metastore tables based clusters.  Though it is possible the ROI on such work may be minimal in the
long run.

It should be possible for the migration to avoid downtime for running queries:
 1. start migration tool: this will build the new metastore changelogs from the old command topic.
    While this tool is left running any updates to the command topic will be applied the metastore
    topics.
 2. Once the initial table state is built, nodes can be upgraded and bounced. Initially, they could
    have a flag set to stop them accepting new DDL statements, (which would suffer from multi-writer
    issues).
 3. Once all nodes updated the migration tool can be stopped and all nodes bounced again, this time
    accepting new DDL statements.

## Performance Implications

Given a key-compacted topic, we expect restart times to be improved and general operating performance
to be unaffected.

## Security Implications

The new metastore changelog topics will need to be secured in the same way as the current command
topic. This should be called out in the docs and we should ensure ksql treats the new topics as
internal / read-only topics,  i.e. users should not be able to modify them via INSERT statements or
use them as a sink for a query.
