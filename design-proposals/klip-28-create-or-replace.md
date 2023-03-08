# KLIP 28 - Introduce CREATE OR REPLACE

**Author**: agavra | 
**Release Target**: 0.12.0; 6.1.0 | 
**Status**: _Merged_ | 
**Discussion**: _https://github.com/confluentinc/ksql/pull/5611_

**tl;dr:** _CREATE OR REPLACE is a mechanism geared toward enabling in-place ksqlDB query evolution._
           
## Motivation and background

Production deployments of databases are never static; they evolve as application and business 
requirements change. To that end, all popular data stores have ways of managing and manipulating 
existing data. For stream processing applications, a user may want to modify their application as 
a result of:

- Business Requirements: requirements simply change over time
- Schema Evolution: the incoming data or required output has been modified
- Optimizations: the same application can be executed more efficiently (either by user or engine)

At time of writing, ksqlDB provides a crude mechanism for altering its application behavior: dropping 
a query and restarting it at the earliest or latest offset. While this often works well in development 
environments, there are limitations to its applicability in production:

- Data Retention: the earliest available offset may not correspond with the beginning of time
- Downtime: the delta between terminating and catch-up may be out of the application’s SLO
- Output Routing: populating to the old output topic will produce duplicates while using a new output topic will require cascading migrations
- Compute: recomputing the complete history for a query may not be feasible

Kafka Streams provides more granular mechanisms (e.g. restarting queries with different behaviors 
but identical consumer groups), but these methods burden users with extra complexity and lack guardrails.

## Scope

To better understand the scope of this KLIP and any future improvements, we define a taxonomy on 
query upgrades as any combination of three types of characteristics: **source query, upgrade** and
(optionally) **environment**:

| **Category** | **Characteristic** | **Description** |
|----------|----------------|-------------|
| Query | Stateful | Stateful queries maintain local storage |
| | Windowed | Windowed queries maintain a limited amount of state specified by a window in time
| | Joined | Joined queries read from multiple sources
| | Multistage | Multistage queries contain intermediate, non-user visible topics in Kafka
| | Nondeterministic | Nondeterministic queries may produce different results when executing identical input
| | Simple | Queries with none of the above characteristics
| Upgrade | Transparent | Transparent upgrades change the way something is computed (e.g. improving a UDF performance)
| | Data Selection | Data selecting query upgrades change which/how many events are emitted
| | Schema Evolution | Schema evolving query upgrades change the output type of the data |
| | Source Modifying | These upgrades change the source data, whether by means of modifying a JOIN or swapping out a source |
| | Topology | These upgrades are invisible to the user, but change the topology, such as the number of sub-topologies or the ordering of operations (e.g. filter push down) |
| | Scaling | Scaling upgrades change the physical properties of the query in order to enable better performance characteristics. |
| | Unsupported | Unsupported upgrades are ones that will semantically change the query in an unsupported way. There are no plans to implement these migrations. |
| Environment | Backfill | Backfill requires the output data to be accurate not just from a point in time, but from the earliest point of retained history |
| | Cascading | Cascading environments contain queries that are not terminal, but rather feed into downstream stream processing tasks |
| | Exactly Once | Exactly Once environments do not allow for data duplication or missed events |
| | Ordered | Ordered environments require that a single offset delineates pre- and post-migration (no events are interleaved) |
| | Live | Live environments describe queries that cannot afford downtime, either by means of acting as live storage (e.g. responding to pull queries) or feeding into high availability systems (powering important functionality) |

### What is in scope

- Specify a syntax that can support arbitrary upgrades
- Design a validator to fail unsupported upgrades
- Design a mechanism for upgrading queries under limited scope

### What is not in scope

The Design section below enumerates which upgrades are out of scope

## Value/Return

This KLIP will represent a significant step forward in the operability of ksqlDB in production, as
noted in the background and motivation section.

## Public APIS

The syntax `CREATE OR REPLACE (STREAM | TABLE) source_name WITH (key=value, ...) AS query;` will be
introduced to allow users to specify an existing stream or table to replace with a new query that
will resume from the same processing point as any previously existing query.

An alternative syntax, `ALTER (STREAM|TABLE) source_table ADD COLUMN col_name col_type 
[, ADD COLUMN col_name col_type...];` will also be introduced to allow the developer to add new
columns to the end of the schema without copying the entire schema. It will function in the exact
same way as `CREATE OR REPLACE`, except that it will only be supported for sources that are not
defined by queries (CAS statements).

## Design

If the `source_name` does not yet exist, a `CREATE OR REPLACE` statement functions identically to
a normal `CREATE` statement. Otherwise, ksqlDB executes the following:

1. Identify the original `queryID` that populates the source (`INSERT INTO` discussed later)
2. Ensure the upgrade is valid
3. Terminate `queryID`
4. Start the new query under the same `queryID`

A few changes need to happen in order to make this work. For 1, we need to maintain a mapping from
source to queryID(s). If the source has multiple associated ids (in the case of `INSERT INTO`) then
the upgrade will fail and not terminate any queries.

For step 2, there will be a component to determine whether two topologies are "upgrade
compatible"; the first iterations, which will be delivered as part of this KLIP, will only allow
for the most basic upgrades: 

- Any _transparent_ upgrade will be supported
- Any _data selection_ upgrade will be supported
- _Schema evolution_ upgrades will be supported on simple and stateful queries, but it will be
    communicated that the users will not get `backfill` or `ordered` properties for stateful.
- _Source modifying_ upgrades will not be supported
- _Topology changes_ will not be supported

Note that _Schema Evolution_ compatibility is defined by the limitations of the serialization
format that is used with the added restrictions against removing fields and changing types to ensure
referential integrity of ksqlDB tables. This way, downstream query output schemas willn not be affected
by upstream schema evolution.

There are currently discussions that discuss how to expand the support of some of these upgrades,
but we believe there is value in supporting the limitted set described above.

For step 4, we need to be able to generate queryIDs differently for `CREATE OR REPLACE` statements
than for others (i.e. it shouldn't just be the offset of the command, but rather the same queryID 
as the original query.) One simple way to implement this is to allow the queryID to be specified
in the command topic. Since the engine receiving the request has a complete view of the engine
metadata, it can determine the queryID to enqueue onto the command topic.

### On "INSERT INTO"

Insert into can eventually be replaced with `UNION` as proposed in [KLIP-17](https://github.com/confluentinc/ksql/pull/4125),
but that must happen in lock-step with this proposal. At first, we will support `CREATE OR REPLACE`
and `INSERT INTO`. Then, we will add support for `UNION`, allowing us to model consecutive `INSERT INTO`
statements as replacing unions with larger unions (essentially adding an extra source to the union).
The approach for that will require a slight modification to the four steps outlined in the design
section above.

## Test plan

QTT tests will be expanded to augmented to support interleaving statements with events. For example:

```json
{
  "statements": [
    [
      "CREATE STREAM foo (col1 int, col2 int) ...;",
      "CREATE STREAM bar AS SELECT col1 FROM foo;"
    ],
    ["CREATE OR REPLACE STREAM bar AS SELECT col1, col2 FROM foo;"]
  ],
  "inputs": [
    [{"topic": "foo", "value": {"col1": 1, "col2": 1}}],
    [{"topic": "foo", "value": {"col1": 2, "col2": 2}}]
  ],
  "outputs": [
    {"topic": "bar", "value": {"col1": 1}},
    {"topic": "bar", "value": {"col1": 2, "col2": 2}}
  ]
}
```

The test would execute in the following order:

1. Issue the first two statements
2. Issue the first input
3. Issue the third statement
4. Issue the second input
5. Ensure output

This can be done in a backwards compatible way and without changing any existing QTT test file
by utilizing `DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY`.

## Documentation Updates

The documentation entries for `CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT` to encompass
the new syntax addition. For example:

```sql
CREATE [OR REPLACE] TABLE table_name
  [WITH ( property_name = expression [, ...] )]
  AS SELECT  select_expr [, ...]
  FROM from_item
  [ LEFT | FULL | INNER ] JOIN join_table ON join_criteria
  [ WINDOW window_expression ]
  [ WHERE condition ]
  [ GROUP BY grouping_expression ]
  [ HAVING having_expression ]
  EMIT CHANGES;
```

Additionally, we will link to a new documentation which describes the limitations in terms of
the query characteristics listed above.

### Description:

If `source_name` exists, this statement is identical to the corresponding `CREATE AS SELECT` 
statement. Otherwise, this statement will replace the existing source with the new query and
begin with the same offsets that the previous existing query was at.

This statement will fail if the upgrade is incompatible, or if the source does not support
upgrades.

## Compatibility Implications

N/A - this introduces new functionality

## Security Implications

N/A
