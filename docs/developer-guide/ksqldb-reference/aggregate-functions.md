---
layout: page
title: ksqlDB Aggregate Functions
tagline:  ksqlDB aggregate functions for queries
description: Aggregate functions to use in  ksqlDB statements and queries
keywords: ksqlDB, function, aggregate
---

## `AVG`

Since: 0.6.0

```sql
AVG(col1)
```

Stream, Table

Return the average value for a given column.

## `COLLECT_LIST`

Since: -

```sql
COLLECT_LIST(col1)
```

Stream, Table

Return an array containing all the values of `col1` from each
input row (for the specified grouping and time window, if any).
Currently only works for simple types (not Map, Array, or Struct).

The size of the result Array can be limited to a maximum of
`ksql.functions.collect_list.limit` entries and any values beyond this 
limit are silently ignored.
When using with a window type of `session`, it can sometimes
happen that two session windows get merged together into one when a
out-of-order record with a timestamp between the two windows is
processed. In this case, the record limit is calculated by
first considering all the records from the first window, then the
out-of-order record, then the records from the second window in
the order they were originally processed.

## `COLLECT_SET`

Since: -

```sql
COLLECT_SET(col1)
```

Stream

Return an array containing the distinct values of `col1` from
each input row (for the specified grouping and time window, if any).
Currently only works for simple types (not Map, Array, or Struct).

The size of the result Array can be limited to a maximum of
`ksql.functions.collect_set.limit` entries and any values beyond this
limit are silently ignored.
When using with a window type of `session`, it can sometimes
happen that two session windows get merged together into one when a
out-of-order record with a timestamp between the two windows is
processed. In this case, the record limit is calculated by
first considering all the records from the first window, then the
out-of-order record, then the records from the second window in
the order they were originally processed.

## `COUNT`

Since: -

```sql
COUNT(col1)
```

```sql
COUNT(*)
```

Stream, Table

Count the number of rows. When `col1` is specified, the count
returned will be the number of rows where `col1` is non-null.
When `*` is specified, the count returned will be the total
number of rows.

## `COUNT_DISTINCT`

Since: 0.7.0

```sql
COUNT_DISTINCT(col1)
```

Stream

Returns the _approximate_ number of unique values of `col1` in a group.
The function implementation uses [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog)
to estimate cardinalities of 10^9 with a typical standard error of 2%.

## `EARLIEST_BY_OFFSET`

Since: 0.10.0

```sql
EARLIEST_BY_OFFSET(col1, [ignoreNulls])
```

Stream

Return the earliest value for the specified column. The earliest value in the partition

has the lowest offset. 


The optional `ignoreNulls` parameter, available since version 0.13.0, controls whether nulls are ignored. The default

is to ignore null values.



Since: 0.13.0

```sql
EARLIEST_BY_OFFSET(col1, earliestN, [ignoreNulls])
```

Stream

Return the earliest _N_ values for the specified column as an `ARRAY`. The earliest values

in the partition have the lowest offsets.


The optional `ignoreNulls` parameter controls whether nulls are ignored. The default

is to ignore null values.


## `HISTOGRAM`

Since: -

```sql
HISTOGRAM(col1)
```

Stream, Table

Return a map containing the distinct String values of `col1`
mapped to the number of times each one occurs for the given window.
This version limits the number of distinct values which can be
counted to 1000, beyond which any additional entries are ignored.
When using with a window type of `session`, it can sometimes
happen that two session windows get merged together into one when a
out-of-order record with a timestamp between the two windows is
processed. In this case the 1000 record limit is calculated by
first considering all the records from the first window, then the
out-of-order record, then the records from the second window in
the order they were originally processed.

## `LATEST_BY_OFFSET`

Since: 0.8.0

```sql
LATEST_BY_OFFSET(col1, [ignoreNulls])
```

Stream

Return the latest value for the specified column. The latest value in the partition

has the largest offset. 


The optional `ignoreNulls` parameter, available since version 0.13.0, controls whether nulls are ignored. The default

is to ignore null values.


Since: 0.13.0

```sql
LATEST_BY_OFFSET(col1, latestN, [ignoreNulls])
```

Stream

Returns the latest _N_ values for the specified column as an `ARRAY`. The latest values have

the largest offset.


The optional `ignoreNulls` parameter controls whether nulls are ignored. The default is to ignore

null values. 

## `MAX`

Since: -

```sql
MAX(col1)
```

Stream

Return the maximum value for a given column and window.
Rows that have `col1` set to null are ignored.

## `MIN`

Since: -

```sql
MIN(col1)
```

Stream

Return the minimum value for a given column and window.
Rows that have `col1` set to null are ignored.

## `STDDEV_SAMP`

Since: - 0.16.0

```sql
STDDEV_SAMP(col1)
```

Stream, Table

Returns the sample standard deviation for the column.

## `SUM`

Since: -

```sql
SUM(col1)
```

Stream, Table

Sums the column values.
Rows that have `col1` set to null are ignored.

## `TOPK`

Since: -

```sql
TOPK(col1, k)
```

Stream

Return the Top *K* values for the given column and window
Rows that have `col1` set to null are ignored.

Example

```sql
SELECT orderzip_code, TOPK(order_total, 5) 
  FROM orders WINDOW TUMBLING (SIZE 1 HOUR)
  GROUP BY order_zipcode
  EMIT CHANGES;
```

## `TOPKDISTINCT`

Since: -

```sql
TOPKDISTINCT(col1, k)
```

Stream

Return the distinct Top *K* values for the given column and window
Rows that have `col1` set to null are ignored.

Example

```sql
SELECT pageid, TOPKDISTINCT(viewtime, 5)
  FROM pageviews_users
  GROUP BY pageid
  EMIT CHANGES;
```
