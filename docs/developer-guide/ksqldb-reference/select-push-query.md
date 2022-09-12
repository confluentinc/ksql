---
layout: page
title: SELECT (Push Query)
tagline:  ksqlDB SELECT statement for push queries
description: Syntax for the SELECT statement in ksqlDB for push queries
keywords: ksqlDB, select, push query
---

## Synopsis

```sql
SELECT select_expr [, ...]
  FROM from_item
  [[ LEFT | FULL | INNER ]
    JOIN join_item
        [WITHIN [<size> <timeunit> | (<before_size> <timeunit>, <after_size> <timeunit>)] [GRACE PERIOD <grace_size> <timeunit>]]
    ON join_criteria]*
  [ WINDOW window_expression ]
  [ WHERE where_condition ]
  [ GROUP BY grouping_expression ]
  [ HAVING having_expression ]
  EMIT [ output_refinement ]
  [ LIMIT count ];
```

## Description

Push a continuous stream of updates to the ksqlDB stream or table. The result of
this statement isn't persisted in a Kafka topic and is printed out only in
the console, or returned to the client. To stop a push query started in the CLI press Ctrl+C.

Execute a push query via the CLI or by sending an HTTP request to the ksqlDB REST API, and
the API sends back a chunked response of indefinite length.

Push queries enable you to subscribe to changes, which enable
reacting to new information in real-time. They’re a good fit for asynchronous
application flows. For request/response flows, see [Pull Queries](select-pull-query.md).

!!! Tip "See push queries in action"
    - [Detect Unusual Credit Card Activity](https://developer.confluent.io/tutorials/credit-card-activity/confluent.html#execute-ksqldb-code)
    - [How to Efficiently Subscribe to a SQL Query for Changes](https://www.confluent.io/blog/push-queries-v2-with-ksqldb-scalable-sql-query-subscriptions/)

Push queries can use all available SQL features, which can be useful when prototyping a
persistent query or when running ad-hoc queries from the CLI. But unlike persistent queries,
push queries are not shared. If multiple clients submit the same push query, ksqlDB computes
independent results for each client.

!!! tip
    If you're using push queries from an application, move all the heavy lifting into a persistent
    query and keep your push query as simple as possible.

In the previous statements, `from_item` is one of the following:

-   `stream_name [ alias ]`
-   `table_name [ alias ]`
-   `from_item LEFT JOIN from_item ON join_condition`

The WHERE clause can refer to any column defined for a stream or table,
including the `ROWTIME`, `ROWPARTITION`, and `ROWOFFSET` pseudo columns.
`where_condition` is an expression that evaluates to true for each record selected.

In the WHERE expression, you can use any operator that ksqlDB supports.
For more information, see
[Operators in ksqlDB](/developer-guide/ksqldb-reference/operators/#operators).

### EMIT

The EMIT clause lets you control the output refinement of your push query. The
output refinement is how you would like to *emit* your results. 

ksqlDB supports the following output refinement types.

#### CHANGES

This is the standard output refinement for push queries, for when you would
like to see all changes happening.

#### FINAL

Use the EMIT FINAL output refinement when you want to emit only the final
result of a windowed aggregation and suppress the intermediate results until
the window closes. This output refinement is supported only for windowed
aggregations.

!!! note
    EMIT `output_refinement` defaults to CHANGES unless explicitly set to FINAL on
    a windowed aggregation.


## Examples

The following statement shows how to select all records from a `pageviews`
stream that have timestamps between two values.

```sql
SELECT * FROM pageviews
  WHERE ROWTIME >= 1510923225000
    AND ROWTIME <= 1510923228000
  EMIT CHANGES;
```

When writing logical expressions using `ROWTIME`, you can use ISO-8601
formatted date strings to represent date times. For example, the previous
query is equivalent to the following:

```sql
SELECT * FROM pageviews
  WHERE ROWTIME >= '2017-11-17T04:53:45'
    AND ROWTIME <= '2017-11-17T04:53:48'
  EMIT CHANGES;
```

If the datestring is inexact, the rest of the timestamp is assumed to be
padded with 0s. For example, `ROWTIME = '2019-07-30T11:00'` is
equivalent to `ROWTIME = '2019-07-30T11:00:00.0000'`.

You can specify time zones within the datestring. For example,
`2017-11-17T04:53:45-0330` is in the Newfoundland time zone. If no
timezone is specified within the datestring, then timestamps are
interpreted in the UTC time zone.

You use the `LIMIT` clause to limit the number of rows returned. Once the
limit is reached, the query terminates.

The following statement shows how to select five records from a `pageviews`
stream.  

```sql
SELECT * FROM pageviews EMIT CHANGES LIMIT 5;
```

If no limit is supplied the query runs until terminated, streaming
back all results to the console.

!!! tip
      If you want to select older data, you can configure ksqlDB to
      query the stream from the beginning. You must run this configuration
      before running the query:

      ```sql
      SET 'auto.offset.reset' = 'earliest';
      ```

### STRUCT output

You can output a [struct](/reference/sql/data-types#struct) from a query
by using a SELECT statement. The following example creates a struct from a
stream named `s1`.

```sql
SELECT STRUCT(f1 := v1, f2 := v2) FROM s1 EMIT CHANGES;
```

### WINDOW

!!! note
    You can use the WINDOW clause only if the `from_item` is a stream.

The WINDOW clause lets you control how to group input records *that have
the same key* into so-called *windows* for operations like aggregations
or joins. Windows are tracked per record key. For more information, see 
[Time and Windows in ksqlDB](/concepts/time-and-windows-in-ksqldb-queries).

Windowing adds two additional system columns to the data, which provide
the window bounds: `WINDOWSTART` and `WINDOWEND`.

ksqlDB supports the following WINDOW types.

#### TUMBLING window

Tumbling windows group input records into fixed-sized,
non-overlapping windows based on the records' timestamps. You must
specify the *window size* for tumbling windows. Tumbling windows are a
special case of hopping windows, where the window size is equal to the
advance interval.

The following statement shows how to create a push query that has a tumbling
window.

```sql
SELECT windowstart, windowend, item_id, SUM(quantity)
  FROM orders
  WINDOW TUMBLING (SIZE 20 SECONDS)
  GROUP BY item_id
  EMIT CHANGES;
```

#### HOPPING window

Hopping windows group input records into fixed-sized,
(possibly) overlapping windows based on the records' timestamps. You
must specify the *window size* and the *advance interval* for
hopping windows.

The following statement shows how to create a push query that has a hopping
window.

```sql
SELECT windowstart, windowend, item_id, SUM(quantity)
  FROM orders
  WINDOW HOPPING (SIZE 20 SECONDS, ADVANCE BY 5 SECONDS)
  GROUP BY item_id
  EMIT CHANGES;
```

#### SESSION window

Session windows group input records into so-called
sessions. You must specify the *session inactivity gap* parameter
for session windows. For example, imagine you set the inactivity gap
to 5 minutes. If, for a given record key such as "alice", no new
input data arrives for more than 5 minutes, then the current session
for "alice" is closed, and any newly arriving data for "alice" in
the future will mark the beginning of a new session.

The following statement shows how to create a push query that has a session
window.

```sql
SELECT windowstart, windowend, item_id, SUM(quantity)
  FROM orders
  WINDOW SESSION (20 SECONDS)
  GROUP BY item_id
  EMIT CHANGES;
```

#### WITHIN and GRACE PERIOD

!!! note
    Stream-Stream joins must have a WITHIN clause specified.

The WITHIN clause lets you specify a time range in a windowed join. When you
join two streams, you must specify a WITHIN clause for matching records that
both occur within a specified time interval.

Here's an example stream-stream-stream join that combines `orders`, `payments` and `shipments`
streams. The resulting ``shipped_orders`` stream contains all orders paid within 1 hour of when
the order was placed, and shipped within 2 hours of the payment being received.

```sql
   CREATE STREAM shipped_orders AS
     SELECT 
        o.id as orderId 
        o.itemid as itemId,
        s.id as shipmentId,
        p.id as paymentId
     FROM orders o
        INNER JOIN payments p WITHIN 1 HOURS ON p.id = o.id
        INNER JOIN shipments s WITHIN 2 HOURS ON s.id = o.id;
```

The GRACE PERIOD, part of the WITHIN clause, allows the join to process out-of-order records for up
to the specified grace period. Events that arrive after the grace period has passed are dropped
as _late_ records and not joined.

```sql
   CREATE STREAM shipped_orders AS
     SELECT
        o.id as orderId
        o.itemid as itemId,
        s.id as shipmentId,
        p.id as paymentId
     FROM orders o
        INNER JOIN payments p WITHIN 1 HOURS GRACE PERIOD 15 MINUTES ON p.id = o.id
        INNER JOIN shipments s WITHIN 2 HOURS GRACE PERIOD 15 MINUTES ON s.id = o.id;
```

If you don't specify a grace period explicitly, the default grace period is 24 hours. This could
cause a huge amount of disk usage on high-throughput streams. Setting a specific GRACE PERIOD is
recommended to reduce high disk usage.

!!! important
    If you specify a GRACE PERIOD for left/outer joins, the grace period defines when the left/outer
    join result is emitted. If you don't specify a GRACE PERIOD for left/outer joins,
    left/outer join results are emitted eagerly, which may cause "spurious" result records, so
    we recommended that you specify a GRACE PERIOD.

#### Out-of-order events

Accept events for up to two hours after the window ends. Events that arrive
after the grace period has passed are dropped and not included in the aggregate
result.

```sql
SELECT orderzip_code, TOPK(order_total, 5) FROM orders
  WINDOW TUMBLING (SIZE 1 HOUR, GRACE PERIOD 2 HOURS) 
  GROUP BY order_zipcode
  EMIT CHANGES;
```
