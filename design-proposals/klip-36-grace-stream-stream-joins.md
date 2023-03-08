# KLIP 36 - GRACE period for stream-stream joins

**Author**: agavra | 
**Release Target**: 0.23.1; 7.1.0 | 
**Status**: _Merged_ | 
**Discussion**: https://github.com/confluentinc/ksql/pull/6175

**tl;dr:** _Support controlling the GRACE period for a stream-stream join to improve disk space
utilization for small windows_
           
## Motivation and background

As described in https://github.com/confluentinc/ksql/issues/6152, the ability to specify
the grace period for stream-stream joins allows more control over the disk space utilization.
This is because a window needs to stay on disk until the grace period has elapsed in order to
ensure that any events that come in timestamped during an old window can be processed as long
as the grace period is active. This means that for windows dramatically smaller than the grace
period (e.g. a minute) we need to keep that level of granularity for the default 24 hour window.

## What is in scope

- Syntax for specifying grace period within stream-stream joins

## What is not in scope

- Deprecation of the old syntax

## Value/Return

Enables onboarding use cases that perform stream-stream joins on high cardinality, high throughput
datasets with small windows without exhausting too much disk space.

## Public APIS

Stream-stream joins require use of `WITHIN` clauses:

```
JOIN <stream> WITHIN <time unit> ON <condition>
```

This will be expanded to support GRACE PERIOD keyword:

Joins with simple windows:
```
JOIN <stream> WITHIN <time unit> GRACE PERIOD <time unit> ON <condition>
```

Joins with before/after windows:
```
JOIN <stream> WITHIN (<time unit>, <time unit>) GRACE PERIOD <time unit> ON <condition>
```

The GRACE PERIOD will be optional for backwards compatibility. Note that using the grace period
will enable a bugfix for left/outer joins. More info in the `Compatibility Implications` section.

## Design

The `WITHIN` clause will be converted into a `KsqlWindowExpression`, and then converted into
a `JoinWindows grace(final Duration afterWindowEnd)` call on the `JoinWindows` in the 
`StreamStreamJoinBuilder`.

## Test plan

We will add the usual QTT tests to ensure that the system respects the new retention limits
and manually test to ensure that we clean up the RocksDB state for expired windows.

## LOEs and Delivery Milestones

Small LOE - under 2 weeks of engineering time to deliver the syntax and implementation.

## Documentation Updates

The entry in `join-streams-and-tables.md` will be updated to include the following:

```md
When you join two streams, you must specify a WITHIN clause for matching
records that both occur within a specified time interval and a grace period. 
The WITHIN clause will specify the "look back" period on the non-triggering stream 
while the grace period will define how out-of-order records will be accepted.

While the grace period is optional, it is recommended to supply a value, otherwise the
default grace period is 24 hours. The local state for a window is not cleaned up
until the grace period has elapsed, so tuning this value to make sense for your specific
application is worthwhile.

For valid time units, see [Time Units](../syntax-reference.md#time-units).

Here's an example stream-stream-stream join that combines `orders`, `payments` and `shipments` 
streams. The resulting ``shipped_orders`` stream contains all orders paid within 1 hour of when
the order was placed, and shipped within 2 hours of the payment being received. 

   CREATE STREAM shipped_orders AS
     SELECT 
        o.id as orderId 
        o.itemid as itemId,
        s.id as shipmentId,
        p.id as paymentId
     FROM orders o
        INNER JOIN payments p WITHIN 1 HOURS ON p.id = o.id
        INNER JOIN shipments s WITHIN 2 HOURS GRACE PERIOD 30 MINUTES ON s.id = o.id;
```

## Compatibility Implications

None. The `GRACE PERIOD` will be optional, so users can continue using the current syntax without
it.

However, using the `GRACE PERIOD` syntax will enable a bugfix in left/outer joins that used to
display spurious results with stream-stream joins. It is optional to the user to set the grace
period for now, but this will be mandatory in the future in order to enforce users to switch
to the kafka streams bugfix. More info: https://issues.apache.org/jira/browse/KAFKA-10847

## Security Implications

N/A

