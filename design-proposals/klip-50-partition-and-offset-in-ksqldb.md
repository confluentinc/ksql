# KLIP 50 - Partition and Offset in ksqlDB

**Author**: Victoria Xia (@vcrfxia) | 
**Release Target**: 0.23.1; 7.1.0 | 
**Status**: _Merged_ | 
**Discussion**: https://github.com/confluentinc/ksql/pull/7505

**tl;dr:** _To aid in development and debugging of ksqlDB applications, ksqlDB should
			allow access to the underlying Kafka topic partition and offset of messages
			during processing. These metadata may also be used to skip select messages
			("poison pills") during processing._

## Motivation and background

As described in the relevant [Github issue](https://github.com/confluentinc/ksql/issues/976),
it's helpful when developing and troubleshooting ksqlDB queries, or when using ksqlDB to
inspect other anomalies in a Kafka topic, to be able to target individual Kafka records.
Today this can be approximated by filtering on the message timestamp via the `ROWTIME`
pseudocolumn in ksqlDB, but this is only approximate as there could be multiple messages
with the same timestamp. In contrast, the combination of topic partition and offset is
guaranteed to uniquely identify a specific message.

## What is in scope

* Syntax for accessing Kafka topic partition and offset
* Desired query semantics (persistent and transient)
* Desired pull query semantics (specifically, that these metadata will not be available 
  by default and must instead be copied into the message value to be accessible)
* Other semantics, such as whether these metadata will be included in source descriptions
  or allowed in `INSERT VALUES` statements
* Compatibility implications for existing ksqlDB queries
* High-level implementation strategy

## What is not in scope

* [Exposing Kafka message headers in ksqlDB](https://github.com/confluentinc/ksql/issues/1940).
  We would like to do this in the future but we'll have a separate KLIP for this as it's a
  larger discussion.
* Updating the output of `PRINT TOPIC` commands include offset (partition is already included).
  This could be nice to do but is orthogonal to this KLIP for exposing additional
  metadata for queries.

## Value/Return

ksqlDB will expose message partition and offset as pseudocolumns similar to the `ROWTIME`
pseudocolumn that exists today. Users will be able to select the partition and offset in
transient queries, persistent queries, and also use the metadata in filters and UDFs.

In all cases, `ROWPARTITION` and `ROWOFFSET` will represent the partition and
offset of messages from the _source_ topic, rather than the output/sink topic.

This will allow users to more easily inspect data and debug ksqlDB queries:
```
SELECT *, ROWPARTITION, ROWOFFSET FROM my_stream EMIT CHANGES;
```
experiment with processing only a subset of messages:
```
SELECT * FROM my_stream WHERE ROWPARTITION = 0 AND ROWOFFSET > 12 EMIT CHANGES;
```
and skip select messages ("poison pills") during processing:
```
CREATE STREAM my_cleaned_stream AS SELECT * FROM my_stream WHERE NOT (ROWPARTITION = 0 AND ROWOFFSET = 42);
```

## Public APIS

We will add two new [pseudocolumns](https://github.com/confluentinc/ksql/blob/master/design-proposals/klip-14-rowtime-as-pseudocolumn.md)
to ksqlDB: `ROWPARTITION` (integer) and `ROWOFFSET` (bigint). We choose these names to be 
consistent with the existing `ROWTIME` pseudocolumn, and to minimize the chance of conflict 
with users' existing data columns for compatibility reasons. See 
["Alternatives Considered"](#alternatives-considered) for discussion on possible alternatives.

### Query Semantics

The behavior of `ROWPARTITION` and `ROWOFFSET` in persistent queries and push queries
will be analogous to those of the [existing `ROWTIME` pseudocolumn](https://github.com/confluentinc/ksql/blob/master/design-proposals/klip-14-rowtime-as-pseudocolumn.md#queries).
In summary:

* `SELECT *` projections will not include `ROWPARTITION` and `ROWOFFSET`
* Users may choose to explicitly add `ROWPARTITION` or `ROWOFFSET` in the selection,
  which will cause ksqlDB to copy the selected pseudocolumn(s) into the output _value_
* `ROWPARTITION` and `ROWOFFSET` will become reserved column names, and users will not
  be allowed to use these names either as source column names or as output column aliases.
  (Existing persistent queries that already use these names will continue to run undisrupted.
  See ["Compatibility Implications"](#compatibility-implications) for more.)

Consistent with `ROWTIME`, the new `ROWPARTITION` and `ROWOFFSET` pseudocolumns will 
also be allowed in `WHERE`, `GROUP BY`, and `PARTITION BY` clauses, though use cases
for the latter two are probably limited. `ROWPARTITION` and `ROWOFFSET` will also be
allowed in UDFs.

In all cases, `ROWPARTITION` and `ROWOFFSET` will represent the partition and
offset of messages from the _source_ topic, rather than the output/sink topic.
Otherwise, it would not be possible for these metedata fields to be used to filter
or limit processing to a specific set of messages. (It's also not possible for a
continuous query to predict the sink topic offset corresponding to a message, in
order to be written as part of the message.)

### Pull Queries

Today, users are able to select the `ROWTIME` pseudocolumn as part of pull queries:
```
SELECT *, ROWTIME FROM my_table WHERE my_key='foo';
```
For multiple reasons, we propose that the new `ROWPARTITION` and `ROWOFFSET` pseudocolumns
should diverge from this behavior. In other words, users will NOT be allowed to select
`ROWPARTITION` or `ROWOFFSET` as part of a pull query.

The first reason is that selecting `ROWPARTITION` or `ROWOFFSET` for a table
is semantically dubious, especially for tables that are the result of other
computation such as an aggregation over a stream. Should `ROWPARTITION` and `ROWOFFSET`
in such a case represent the partition and offset of the message in the source stream
that resulted in the aggregate value, or should they represent the partition and offset
of the aggregate value in the output table? Note that timestamps don't have this
ambiguity as the timestamp of the two records mentioned are the same.

The second reason has to do with implications on state store size. ksqlDB is able to 
support `ROWTIME` in pull queries because the state stores from which pull queries are 
served includes the timestamp for the latest table row (for each key). In contrast, 
message partition and offset are not saved in the state stores. As a result, supporting 
`ROWPARTITION` and `ROWOFFSET` in pull queries would require adding this information
to state stores. (I think there might be a way to get the partition by other means,
from the replica queried in order to serve the pull query, but offset information
definitely wouldn't be available.)

If a user really wants to make partition and/or offset information available for pull
queries, they can choose to explicitly include the source partition and offset as
part of the query that materializes their table and query for partition/offset as
value columns as part of their pull query:
```
CREATE TABLE table_with_metadata AS SELECT *, ROWPARTITION AS partition, ROWOFFSET AS offset FROM source_table;
SELECT col1, col2, partition, offset FROM table_with_metadata WHERE my_key='foo';
```

### Other semantics

Similar to `ROWTIME`, the new `ROWPARTITION` and `ROWOFFSET` pseudocolumns will not 
be included in the list of columns returned as part of a source description.

Unlike `ROWTIME`, the new `ROWPARTITION` and `ROWOFFSET` pseudocolumns will not
be allowed as part of `INSERT VALUES` statements. It does not make sense for users
to specify the offset, and while it's technically possible for users to specify a
custom partition, this does not seem useful as violating expected partitioning has
implications for downstream queries.

## Design

`ROWPARTITION` and `ROWOFFSET` will be added as new pseudocolumns, similar to how
`ROWTIME` is already implemented today. New logic will be added to disallow the use of
`ROWPARTITION` and `ROWOFFSET` in pull queries, while `ROWTIME` will still be allowed.

However, directly adding `ROWPARTITION` and `ROWOFFSET` as new pseudocolumns would break 
compatibility for certain existing queries since, in some situations, 
[ksqlDB writes pseudocolumns into repartition and changelog topics](https://github.com/confluentinc/ksql/issues/7489). 
This is problematic because adding new pseudocolumns would change the schema of these 
intermediate topics (and state stores) and break running queries as a result.

In light of the above, we have two approaches for implementation:
1. Update ksqlDB to never write pseudocolumns into repartition and changelog topics 
   (unless they are needed as part of the query downstream), i.e., address 
   https://github.com/confluentinc/ksql/issues/7489. Then we can freely add new 
   pseudocolumns without breaking compatibility for existing queries.
2. Defer on updating ksqlDB to not write pseudocolumns into intermediate topics and 
   instead modify the stream and table source execution steps to include something 
   to indicate the set of pseudocolumns that should be included with the source. 
   Specifically, we can add a "pseudocolumn version number" field into the execution 
   step. By increasing the version number when new pseudocolumns are added, old queries 
   will continue to run with the old set of pseudocolumns and therefore maintain 
   compatibility, while new queries will have new pseudocolumns.

Given that we'd like to start work on exposing partition and offset within the next month,
my preference is for Option 2. The drawback is that intermediate topics for pre-join
repartitions and changelogs will now have two additional columns (for partition and offset).
I think this is fine for now since partition and offset are relatively small (integer and
bigint, respectively) but we should address https://github.com/confluentinc/ksql/issues/7489
before [headers are added](https://github.com/confluentinc/ksql/issues/1940) as an 
additional pseudocolumn.

## Test plan

Query translation tests will be added to cover the following.

New functionality:
* Push queries may select `ROWPARTITION` and/or `ROWOFFSET`, including in expressions
* Persistent queries may select `ROWPARTITION` and/or `ROWOFFSET`, including in expressions,
  as long as the result columns are properly aliased
* Queries may use `ROWPARTITION` and/or `ROWOFFSET` in filters
* Queries may use `ROWPARTITION` and/or `ROWOFFSET` in UDFs
* Queries may use `ROWPARTITION` and/or `ROWOFFSET` in `GROUP BY` and `PARTITION BY` clauses

Negative test cases:
* New queries that use either `ROWPARTITION` or `ROWOFFSET` as source column names
  are disallowed
* New queries that use either `ROWPARTITION` or `ROWOFFSET` as output column aliases
  are disallowed
* Pull queries that attempt to select `ROWPARTITION` or `ROWOFFSET` are disallowed
* `ROWPARTITION` and `ROWOFFSET` do not appear in the list of columns returned by
  describe source commands

Backwards compatibility:
* Existing persistent queries that use either `ROWPARTITION` or `ROWOFFSET` as source
  column names are unaffected
* Existing persistent queries that use either `ROWPARTITION` or `ROWOFFSET` as output
  column aliases are unaffected
* Other existing persistent queries (i.e., QTT historic plans) are unaffected
* With the exception of [persistent queries involving joins followed by `PARTITION BY`](https://github.com/confluentinc/ksql/issues/7488),
  no output schemas for persistent queries should change

## LOEs and Delivery Milestones

Development of this feature will proceed hidden behind a feature flag until the
feature is fully ready.

Implementation steps are:
* Add feature flag
* Add `ROWPARTITION` and `ROWOFFSET` pseudocolumns
* Lift restriction for reserved pseudocolumn names when executing statements from
  the command topic, in order to preserve backwards compatibility for existing statements
* Update source stream and table execution steps to version the set of pseudocolumns
  in use at any given time
  * Add versioning scheme and new field to execution steps
  * Ensure proper deserialization of existing execution steps without the new field
* Update SourceBuilder to use the new pseudocolumn version
  * Update logical schema generation to include the new pseudocolumns
  * Extract metadata from Kafka messages to populate the new pseudocolumns
* Update other instances where internal schemas need to accommodate the new
  pseudocolumns, and ensure everything is properly wired together
* Disallow pull queries that include `ROWPARTITION` or `ROWOFFSET`
* Disallow `ROWPARTITION` or `ROWOFFSET` in `INSERT VALUES` statements
* Remove feature flag

## Documentation Updates

The [docs page on pseudocolumns](https://docs.ksqldb.io/en/latest/reference/sql/data-definition/#pseudocolumns)
will be updated to reflect the addition of `ROWPARTITION` and `ROWOFFSET`.

We will also add a section explaining that, in contrast to `ROWTIME`, the 
`ROWPARTITION` and `ROWOFFSET` pseudocolumns may not be queried as part of pull
queries, but there is a workaround for users who want this capability
(see the example in the [section on "Pull Queries"](#pull-queries) above).

## Compatibility Implications

As described in the section on ["Design"](#design), existing DDL and DML statements,
including those that use `ROWPARTITION` or `ROWOFFSET` as a source column name
or sink column alias, will be unaffected. New queries that involving columns named
`ROWPARTITION` or `ROWOFFSET` will be rejected.

## Security Implications

N/A

## Alternatives Considered

As discussed [previously](https://github.com/confluentinc/ksql/issues/3734), rather than
having separate pseudocolumns for each of `ROWTIME`, `ROWPARTITION`, and `ROWOFFSET`,
we could instead have a single reserved column (e.g., `METADATA`) that includes each
of the columns, and users could access individual columns via struct dereference syntax:
```
SELECT METADATA->ROWTIME, METADATA->ROWPARTITION, METADATA->ROWOFFSET FROM my_stream EMIT CHANGES;
```

We could make this change if we want but the syntax is a bit clunkier.

We also discussed using function notation to retrieve metadata, rather than having
pseudocolumns:
```
SELECT ROWTIME(), ROWPARTITION(), ROWOFFSET() FROM my_stream EMIT CHANGES;
```
but this was vetoed as the syntax does not make sense in the context of joins,
where users should be able to select metadata from individual join sources.

