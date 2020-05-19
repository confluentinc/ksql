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
- Outline of potential implementation

**Out of Scope**:
- Grace periods for windows
- Custom suppressions

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
CHANGES | FINAL [EAGERLY] [WHEN suppression]
```

And behaves in the following ways:
- If `CHANGES` is specified, then all intermediate changes will be materialized
- If `FINAL` is specified, then output will be suppressed depending on the `suppression`:
    - if `EAGERLY` is specified, the suppression is a best effort attempt to reduce duplicate output
      data, but no guarantees are made if a constraint is met (e.g. buffer fills up)
    - if `EAGERLY` is omitted, then the query will fail if the buffer fills up before the refinement
      condition is met

A `suppression` will implement `Suppressed`. KSQL will provide two builtin suppression mechanisms,
with `WINDOW_CLOSED()` being the default if no suppression is supplied:
- `WINDOW_CLOSED()` will create a suppression policy that emits events when the window is closed
- `TIME_LIMIT(time_expression)` will create a suppression policy that emits events when a certain
  time period has passed since seeing a given key for the first time

We will model the suppression as method calls in order to allow flexibility in the syntax if
different types of suppressions are added in the future. We could also extend this to allow user
implemented suppressions in the same way that we support UDFs.

## Design

We will add a `SuppressNode` to the Logical Plan of persistent queries that specify anything
other than `EMIT CHANGES`. The implementation will translate to adding the `.suppress` on to the
underlying `KTable` when building the Physical plan.

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
