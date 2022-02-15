---
layout: page
title: ksqlDB Aggregate Functions
tagline:  ksqlDB aggregate functions for queries
description: Aggregate functions to use in SQL statements and queries
keywords: ksqlDB, SQL, function, aggregate
---

## **`AVG`**

```sql title="Applies to: stream, table<br>Since: 0.6.0"
AVG(col1)
```

Returns the average value for `col1`.

!!! Tip "See AVG in action"
    - [Build a dynamic pricing strategy](https://confluentinc.github.io/ksqldb-recipes/real-time-analytics/dynamic-pricing/#ksqldb-code)

---

## **`COLLECT_LIST`**

```sql title="Applies to: stream, table<br>"
COLLECT_LIST(col1) => ARRAY
```

Returns an array containing all the values of `col1` from each
input row (for the specified grouping and time window, if any).

The `COLLECT_LIST` function works only for simple types and doesn't support
complex types like MAP, ARRAY, and STRUCT.

The size of the result ARRAY can be limited to a maximum of
`ksql.functions.collect_list.limit` entries, and any values beyond this
limit are ignored silently.

When used with `SESSION` window, sometimes two session windows are merged
together into one, when a out-of-order record with a timestamp between
the two windows is processed. In this case, the record limit is calculated by
first considering all the records from the first window, then the out-of-order
record, then the records from the second window in the order they were
originally processed.

!!! Tip "See COLLECT_LIST in action"
    - [Automate instant payment verifications](https://confluentinc.github.io/ksqldb-recipes/real-time-analytics/payment-status-check/#ksqldb-code)

---

## **`COLLECT_SET`**

```sql title="Applies to: stream<br>"
COLLECT_SET(col1) => ARRAY
```

Returns an array containing the distinct values of `col1` from
each input row (for the specified grouping and time window, if any).

The `COLLECT_SET` function works only for simple types and doesn't support
complex types like MAP, ARRAY, and STRUCT.

The size of the result ARRAY can be limited to a maximum of
`ksql.functions.collect_set.limit` entries, and any values beyond this
limit are ignored silently.

When used with a `SESSION` window, sometimes two session windows are merged
together into one, when a out-of-order record with a timestamp between
the two windows is processed. In this case, the record limit is calculated by
first considering all the records from the first window, then the out-of-order
record, then the records from the second window in the order they were
originally processed.

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
    - [Build Customer Loyalty Programs](https://confluentinc.github.io/ksqldb-recipes/customer-360/loyalty-rewards/#ksqldb-code)
    - [Understand user behavior with clickstream data](https://confluentinc.github.io/ksqldb-recipes/customer-360/clickstream/#ksqldb-code)

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
    - [Automate instant payment verifications](https://confluentinc.github.io/ksqldb-recipes/real-time-analytics/payment-status-check/#ksqldb-code)

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
    - [Build a dynamic pricing strategy](https://confluentinc.github.io/ksqldb-recipes/real-time-analytics/dynamic-pricing/#ksqldb-code)
    - [Detect Unusual Credit Card Activity](https://confluentinc.github.io/ksqldb-recipes/anomaly-detection/credit-card-activity/#ksqldb-code)

---

## **`MIN`**

```sql title="Applies to: stream<br>"
MIN(col1)
```

Returns the minimum value for a given column and window.

Rows that have `col1` set to `NULL` are ignored.

!!! Tip "See MIN in action"
    - [Build a dynamic pricing strategy](https://confluentinc.github.io/ksqldb-recipes/real-time-analytics/dynamic-pricing/#ksqldb-code)

---

## **`STDDEV_SAMP`**

```sql title="Applies to: stream, table<br>Since: 0.16.0"
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
    - [Detect Unusual Credit Card Activity](https://confluentinc.github.io/ksqldb-recipes/anomaly-detection/credit-card-activity/#ksqldb-code)
    - [Build Customer Loyalty Programs](https://confluentinc.github.io/ksqldb-recipes/customer-360/loyalty-rewards/#ksqldb-code)

---

## **`TOPK`**

```sql title="Applies to: stream<br>"
TOPK(col1, k)
```

Returns the Top *K* values for the given column and window.

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
