---
layout: page
title: ksqlDB Aggregate Functions
tagline:  ksqlDB aggregate functions for queries
description: Aggregate functions to use in SQL statements and queries
keywords: ksqlDB, SQL, function, aggregate
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/aggregate-functions.html';
</script>

!!! Important
    In an aggregation function, providing a `*` character or an empty argument
    list causes the function to return only the ROWTIME column. For example,
    calling `AVG(*)` or `AVG()`  returns the  average of ROWTIME.

## **`AVG`**

```sql title="Applies to: stream, table<br>Since: 0.6.0"
AVG(col1)
```

Returns the average value for `col1`.

!!! Tip "See AVG in action"
    - [Build a dynamic pricing strategy](https://developer.confluent.io/tutorials/dynamic-pricing/confluent.html#execute-ksqldb-code)

---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/aggregate-functions.html#collect_list';
</script>

## **`COLLECT_LIST`**

```sql title="Applies to: stream, table<br>"
COLLECT_LIST(col1) => ARRAY
```

Returns an array containing all the values of `col1` from each
input row (for the specified grouping and time window, if any).

The size of the result ARRAY can be limited to a maximum of
`ksql.functions.collect_list.limit` entries, and any values beyond this
limit are ignored silently.

!!! note

    In {{ site.ccloud }}, the `ksql.functions.collect_list.limit` config is set
    to 1000 and can't be changed.

When used with `SESSION` window, sometimes two session windows are merged
together into one, when a out-of-order record with a timestamp between
the two windows is processed. In this case, the record limit is calculated by
first considering all the records from the first window, then the out-of-order
record, then the records from the second window in the order they were
originally processed.

!!! Tip "See COLLECT_LIST in action"
    - [Automate instant payment verifications](https://developer.confluent.io/tutorials/payment-status-check/confluent.html#execute-ksqldb-code)

---

## **`COLLECT_SET`**

```sql title="Applies to: stream<br>"
COLLECT_SET(col1) => ARRAY
```

Returns an array containing the distinct values of `col1` from
each input row (for the specified grouping and time window, if any).

The size of the result ARRAY can be limited to a maximum of
`ksql.functions.collect_set.limit` entries, and any values beyond this
limit are ignored silently.

!!! note

    In {{ site.ccloud }}, the `ksql.functions.collect_set.limit` config is set
    to 1000 and can't be changed.

When used with a `SESSION` window, sometimes two session windows are merged
together into one, when a out-of-order record with a timestamp between
the two windows is processed. In this case, the record limit is calculated by
first considering all the records from the first window, then the out-of-order
record, then the records from the second window in the order they were
originally processed.

---

## **`CORRELATION`**

```sql title="Applies to: stream, table<br>Since: 0.29.0"
CORRELATION(x, y)
```

Returns the Pearson correlation coefficient between columns `x` and `y`. If either value in `x` or `y` is `NULL` for a particular row, that row is ignored.

If all rows contain `NULL` for `x` or `y`, or if there is only one non-null row, `NaN` is returned. When there are only two non-null rows, either `1.0` or `-1.0` is returned, depending on the sign of the slope of the line that would be drawn between the two points.

If all the values in one column are equal, `NaN` is returned.

---

## **`COUNT`**

```sql title="Applies to: stream, table<br>"
COUNT(col1)
COUNT(*)
```

Counts the number of rows.

When `col1` is specified, the count returned is the number of rows where
`col1` is non-null.

When `*` is specified, the count returned is the total number of rows.

!!! Tip "See COUNT in action"
    - [Build Customer Loyalty Programs](https://developer.confluent.io/tutorials/loyalty-rewards/confluent.html#execute-ksqldb-code)
    - [Understand user behavior with clickstream data](https://developer.confluent.io/tutorials/clickstream/confluent.html#execute-ksqldb-code)

---

## **`COUNT_DISTINCT`**

```sql  title="Applies to: stream<br>Since: 0.7.0"
COUNT_DISTINCT(col1)
```

Returns the _approximate_ number of unique values of `col1` in a group.

The function implementation uses [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog)
to estimate cardinalities of 10^9 with a typical standard error of 2 percent.

---

## **`EARLIEST_BY_OFFSET`**

```sql title="Applies to: stream<br>Since: 0.10.0"
EARLIEST_BY_OFFSET(col1, [ignoreNulls])
```

Return the earliest value for the specified column.

The earliest value in the partition has the lowest offset. 

The optional `ignoreNulls` parameter, available since version 0.13.0,
controls whether `NULL` values are ignored. The default is to ignore
`NULL` values.

```sql  title="Applies to: stream<br>Since: 0.13.0"
EARLIEST_BY_OFFSET(col1, earliestN, [ignoreNulls])
```

Returns the earliest _N_ values for the specified column as an `ARRAY`.

The earliest values in the partition have the lowest offsets.

The optional `ignoreNulls` parameter, available since version 0.13.0,
controls whether `NULL` values are ignored. The default is to ignore
`NULL` values.

---

## **`HISTOGRAM`**

```sql title="Applies to: stream, table<br>"
HISTOGRAM(col1)
```

Returns a map containing the distinct string values of `col1`
mapped to the number of times each one occurs for the given window.

The `HISTOGRAM` function limits the number of distinct values which can be
counted to 1000, beyond which any additional entries are ignored.

When used with a `SESSION` window, sometimes two session windows are merged
together into one, when a out-of-order record with a timestamp between
the two windows is processed. In this case, the 1000-record limit is calculated
by first considering all the records from the first window, then the
out-of-order record, then the records from the second window in the order they
were originally processed.

!!! Tip "See HISTOGRAM in action"
    - [Automate instant payment verifications](https://developer.confluent.io/tutorials/payment-status-check/confluent.html#execute-ksqldb-code)

---

## **`LATEST_BY_OFFSET`**

```sql title="Applies to: stream<br>Since: 0.8.0"
LATEST_BY_OFFSET(col1, [ignoreNulls])
```

Returns the latest value for the specified column.

The latest value in the partition has the largest offset. 

The optional `ignoreNulls` parameter, available since version 0.13.0,
controls whether `NULL` values are ignored. The default is to ignore
`NULL` values.

```sql title="Applies to: stream<br>Since: 0.13.0"
LATEST_BY_OFFSET(col1, latestN, [ignoreNulls])
```

Returns the latest _N_ values for the specified column as an `ARRAY`.

The latest values have the largest offset.

The optional `ignoreNulls` parameter, available since version 0.13.0,
controls whether `NULL` values are ignored. The default is to ignore
`NULL` values.

---

## **`MAX`**

```sql title="Applies to: stream<br>"
MAX(col1)
```

Returns the maximum value for a given column and window.

Rows that have `col1` set to `NULL` are ignored.

!!! Tip "See MAX in action"
    - [Build a dynamic pricing strategy](https://developer.confluent.io/tutorials/dynamic-pricing/confluent.html#execute-ksqldb-code)
    - [Detect Unusual Credit Card Activity](https://developer.confluent.io/tutorials/credit-card-activity/confluent.html#execute-ksqldb-code)

---

## **`MIN`**

```sql title="Applies to: stream<br>"
MIN(col1)
```

Returns the minimum value for a given column and window.

Rows that have `col1` set to `NULL` are ignored.

!!! Tip "See MIN in action"
    - [Build a dynamic pricing strategy](https://developer.confluent.io/tutorials/dynamic-pricing/confluent.html#execute-ksqldb-code)

---

## **`STDDEV_SAMP`**

```sql title="Applies to: stream, table<br>Since: 0.16.0"
STDDEV_SAMP(col1)
```
!!! Note: This function returns the square of the standard deviation instead of

the standard deviation. Use the `STDDEV_SAMPLE` function to compute the standard deviation.  


Returns the sample standard deviation for the column.

---

## **`STDDEV_SAMPLE`**

```sql title="Applies to: stream, table<br>Since: 0.25.0"
STDDEV_SAMP(col1)
```

Returns the sample standard deviation for the column.

---

## **`SUM`**

```sql title="Applies to: stream, table<br>"
SUM(col1)
```
Sums the column values.

Rows that have `col1` set to `NULL` are ignored.

!!! Tip "See SUM in action"
    - [Detect Unusual Credit Card Activity](https://developer.confluent.io/tutorials/credit-card-activity/confluent.html#execute-ksqldb-code)
    - [Build Customer Loyalty Programs](https://developer.confluent.io/tutorials/loyalty-rewards/confluent.html#execute-ksqldb-code)

---

## **`TOPK`**

```sql title="Applies to: stream<br>"
TOPK(col1, otherCols..., k)
```

Returns the Top *K* values for the given column and window.

If only `col1` is provided, an array of values for that column is
returned. If `otherCols` are provided, a list of `STRUCT`s is returned. Each
`STRUCT` has a field named `sort_col` that contains value of `col1` in the 
associated record. `otherCols` are in fields named `col0`, `col1`, `col2`, 
etc., in the order they were provided. `otherCols` do not all have to
be of the same type.

Rows that have `col1` set to `NULL` are ignored.

```sql title="Example", hl_lines="1"
SELECT orderzip_code, TOPK(order_total, 5) 
  FROM orders WINDOW TUMBLING (SIZE 1 HOUR)
  GROUP BY order_zipcode
  EMIT CHANGES;
```

---

## **`TOPKDISTINCT`**

```sql title="Applies to: stream<br>"
TOPKDISTINCT(col1, k)
```

Returns the distinct Top *K* values for the given column and window.

Rows that have `col1` set to `NULL` are ignored.

```sql title="Example", hl_lines="1"
SELECT pageid, TOPKDISTINCT(viewtime, 5)
  FROM pageviews_users
  GROUP BY pageid
  EMIT CHANGES;
```
