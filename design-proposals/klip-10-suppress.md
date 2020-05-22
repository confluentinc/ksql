# KLIP 10 - Add Suppress To KSQL

**Author**: agavra | 
**Release Target**: 5.5 | 
**Status**: _In Discussion_ | 
**Discussion**: _link to the design discussion PR_

**tl;dr:** _There have been many requests from the community to be able to the control continuous 
           refinement policy of underlying KStreams applications. `KTable#suppress` allows such
           control and should be given corresponding KSQL syntax._
           
## Motivation and background

Inspired by: [Kafka Streams' Take on Watermarks and Triggers](
https://www.confluent.io/blog/kafka-streams-take-on-watermarks-and-triggers)

KSQL provides mechanisms to aggregate data and maintain an internal materialization. As a motivating
example, let's consider an alerting mechanism built on KSQL. We begin with a stream `healthcheck` 
that has the schema `server VARCHAR`. Servers emit to this stream every minute with their own name
to indicate that they are up and healthy. We create a KSQL application from this stream:

```sql 
CREATE TABLE alerts AS
    SELECT COUNT(*) as count, server
    FROM healthcheck
    WINDOW TUMBLING (SIZE 5 MINUTES) 
    GROUP BY server
    EMIT CHANGES;
```

We have a downstream microservice that consumes from the `alerts` topic and sends an email whenever
the count is less than 3 for a server (i.e. a server missed 2 out of 5 heartbeat healthchecks in a
window of 5 minutes). 

Today, building such an application would not work the way you want it to! The downstream service
would get intermediate results (the exact results are slightly unpredictable, as they depend on
the `commit.interval.ms` and `cache.max.bytes.buffering` configurations), and would spuriously
alert. For example, if the `healtcheck` stream had the following data:
```
ts: min_1 server: abc
ts: min_2 server: abc
ts: min_3 server: abc
```
the `alerts` topic would get the update three times (each time with incrementing `count` column 
value), and would send three emails when none should be sent.

## Scope

**In Scope**: 
- Outline the features of suppress that need to be represented in KSQL
- Syntax for suppress functionality
- Grace periods for windows

**Out of Scope**:
- Outline of potential implementation

## Value/Return

This feature enables functionality in KSQL that is not possible today, and provides stricter
semantic guarantees on KSQL aggregation queries.

## Public APIS

The public API will extend the `EMIT` clause to support refinement operators other than `CHANGES`
(which is the only currently supported refinement). The proposed syntax is:

```sql
SELECT select_expression
    FROM source
    WINDOW window_expression
    GROUP BY grouping_expression
    EMIT refinement_expression
```

A `refinement_expression` has the following syntax:
```sql
CHANGES | FINAL
```

And behaves in the following ways:
- If `CHANGES` is specified, then all intermediate changes will be materialized
- If `FINAL` is specified, then output will be emitted only when the aggregation window has closed
    - the `AFTER` clause allows the user to specify the grace period as part of the syntax
    
Since the default grace period in Kafka Streams is 24 hours, the default `EMIT FINAL` behavior will
only omit data 24 hours after the query starts. Since this is unlikely to be good user experience,
this KLIP proposes to set a default grace period of 0 for all queries.

NOTE: prior to this KLIP, the grace period can be controlled by 

## Discussion

Consider the following query hierarchy: there are _persistent_ and _transient_ queries. Within the
class of _transient_ queries, there are _push_ and _pull_ queries.

KLIP-8 proposed using `EMIT CHANGES` to specify a push query. This KLIP diverges slightly to propose:

- The presence of a `CREATE` clause in a statement indicates a persistent query. Since all persistent queries
    are continuous, they must specify a refinement expression. For a streamlined user experience, 
    `EMIT CHANGES` is added if the user omits one. This can change later when we add the distinction 
    between `MATERIALIZED VIEW` and normal sources.
- If a statement lacks a `CREATE` clause, the presence of any refinement expression (`EMIT`) indicates
    a push query.
- If the statement has neither a `CREATE` clause nor a refinement expression, then it is a pull query.

This proposal leverages the dichotomy between `CHANGES` and `FINAL` to draw a distinction between an 
open and closed window aggregation. What does this mean when applied to a source that is not windowed,
such as an un-windowed table or a stream? A table without a window is never final, therefore `EMIT FINAL` 
on such a source will never return any data.

Streams are a little more complicated. Since streams do not support pull queries today we could handle
this in one of three ways:

1. Require all queries on streams to meet one of the criteria above that force a continuous query
    (either a `CREATE` or a refinement expression). The problem here is that a refinement expression
    doesn't really make sense on a stream as there is no dichotomy between changes and final.
2. Implicitly create continuous queries for all queries on streams. The problem here is that this closes
    the door to support pull queries on streams (i.e. queries which return a table without performing any
    aggregation).
3. Introduce new syntax to specify a pull query on a stream.

This KLIP proposes to punt this discussion and choose the first option as it is backwards compatible
with what exists today.

   
## Test plan

- Add corresponding QTTs will cover this functionality 

## Documentation Updates

See the `Public APIs` section above.

# Compatibility Implications

N/A

## Performance Implications

N/A

## Security Implications

N/A
