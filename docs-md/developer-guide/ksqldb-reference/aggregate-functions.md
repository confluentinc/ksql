---
layout: page
title: ksqlDB Aggregate Functions
tagline:  ksqlDB aggregate functions for queries
description: Aggregate functions to use in  ksqlDB statements and queries
keywords: ksqlDB, function, aggregate
---

Aggregate Functions
===================

For more information, see
[Aggregate Streaming Data With ksqlDB](../aggregate-streaming-data.md).

  - [AVG](#average)
  - [COLLECT_LIST](#collectlist)
  - [COLLECT_SET](#collectset)
  - [COUNT](#count)
  - [HISTOGRAM](#histogram)
  - [MAX](#max)
  - [MIN](#min)
  - [SUM](#sum)
  - [TOPK](#topk)
  - [TOPKDISTINCT](#topkdistinct)
  - [WindowStart](#windowstart)
  - [WindowEnd](#windowend)

AVG
---

`AVG(col1)`

Stream, Table

Return the average value for a given column.

COLLECT_LIST
------------

`COLLECT_LIST(col1)`

Stream, Table

Return an array containing all the values of `col1` from each
input row (for the specified grouping and time window, if any).
Currently only works for simple types (not Map, Array, or Struct).
This version limits the size of the result Array to a maximum of
1000 entries and any values beyond this limit are silently ignored.
When using with a window type of `session`, it can sometimes
happen that two session windows get merged together into one when a
late-arriving record with a timestamp between the two windows is
processed. In this case the 1000 record limit is calculated by
first considering all the records from the first window, then the
late-arriving record, then the records from the second window in
the order they were originally processed.


COLLECT_SET
-----------

`COLLECT_SET(col1)`

Stream

Return an array containing the distinct values of `col1` from
each input row (for the specified grouping and time window, if any).
Currently only works for simple types (not Map, Array, or Struct).
This version limits the size of the result Array to a maximum of
1000 entries and any values beyond this limit are silently ignored.
When using with a window type of `session`, it can sometimes
happen that two session windows get merged together into one when a
late-arriving record with a timestamp between the two windows is
processed. In this case the 1000 record limit is calculated by
first considering all the records from the first window, then the
late-arriving record, then the records from the second window in
the order they were originally processed.


COUNT
-----

`COUNT(col1)`, 
`COUNT(*)`     

Stream, Table

Count the number of rows. When `col1` is specified, the count
returned will be the number of rows where `col1` is non-null.
When `*` is specified, the count returned will be the total
number of rows.

COUNT_DISTINCT
--------------

`COUNT_DISTINCT(col1)`

Stream, Table

Returns the _approximate_ number of unique values of `col1` in a group.
The function implementation uses [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog)
to estimate cardinalties of 10^9 with a typical standard error of 2%.


HISTOGRAM
---------

`HISTOGRAM(col1)`

Stream, Table

Return a map containing the distinct String values of `col1`
mapped to the number of times each one occurs for the given window.
This version limits the number of distinct values which can be
counted to 1000, beyond which any additional entries are ignored.
When using with a window type of `session`, it can sometimes
happen that two session windows get merged together into one when a
late-arriving record with a timestamp between the two windows is
processed. In this case the 1000 record limit is calculated by
first considering all the records from the first window, then the
late-arriving record, then the records from the second window in
the order they were originally processed.

LATEST_BY_OFFSET
----------------

`LATEST_BY_OFFSET(col1)`

Stream

Return the latest value for a given column. Latest here is defined as the value in the partition
with the greatest offset.
Note: rows where `col1` is null will be ignored.

MAX
---

`MAX(col1)`

Stream

Return the maximum value for a given column and window.
Note: rows where `col1` is null will be ignored.

MIN
---

`MIN(col1)`

Stream

Return the minimum value for a given column and window.
Note: rows where `col1` is null will be ignored.

SUM
---

`SUM(col1)`

Stream, Table

Sums the column values.
Note: rows where `col1` is null will be ignored.

TOPK
----

`TOPK(col1, k)`

Stream

Return the Top *K* values for the given column and window
Note: rows where `col1` is null will be ignored.

TOPKDISTINCT
------------

`TOPKDISTINCT(col1, k)`

Stream

Return the distinct Top *K* values for the given column and window
Note: rows where `col1` is null will be ignored.


Page last revised on: {{ git_revision_date }}
