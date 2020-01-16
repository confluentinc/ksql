---
layout: page
title: Aggregate Streaming Data With ksqlDB
tagline: Build stateful aggregates on streaming data
description: Learn about aggregation functions in ksqlDB statements 
---

ksqlDB supports several aggregate functions, like COUNT and SUM. You can
use these to build stateful aggregates on streaming data. For the full
list, see [Aggregate functions](ksqldb-reference/aggregate-functions.md).

Here are some examples that show how to aggregate data from an inbound
stream of pageview records, named `pageviews`.

Aggregate Results in a ksqlDB Table
---------------------------------

The result of an aggregate query in ksqlDB is always a table, because ksqlDB
computes the aggregate for each key, and possibly for each window per
key, and updates these results as it processes new input data for a key.

![A ksqlDB table aggregating results of a per-region count on a stream](../img/ksql-stream-table-numVisitedLocations.gif)

Assume that you want to count the number of pageviews per region. The
following query uses the COUNT function to count the pageviews from the
time you start the query until you terminate it. The query uses the
CREATE TABLE AS SELECT statement, because the result of the query is a
ksqlDB table.

```sql
CREATE TABLE pageviews_per_region AS
  SELECT regionid,
         COUNT(*)
  FROM pageviews
  GROUP BY regionid
  EMIT CHANGES;
```

### Tombstone Records

An important difference between tables and streams is that a record with
a non-null key and a null value has a special semantic meaning: in a
table, this kind of record is a *tombstone*, which tells ksqlDB to "DELETE
this key from the table". For a stream, `null` is a value like any other,
with no special meaning.

Aggregate Over Windows
----------------------

ksqlDB supports aggregation using tumbling, hopping, and session windows.

In a windowed aggregation, the first seen message is written into the
table for a particular key as a null. Downstream applications reading
the data will see nulls, and if an application can't handle null
values, it may need a separate stream that filters these null records
with a WHERE clause.

### Aggregate Records Over a Tumbling Window

This query computes the pageview count per region per minute:

```sql
CREATE TABLE pageviews_per_region_per_minute AS
  SELECT regionid,
         COUNT(*)
  FROM pageviews
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY regionid
  EMIT CHANGES;
```

To count the pageviews for "Region_6" by female users every 30 seconds,
you can change the previous query to the following:

```sql
CREATE TABLE pageviews_per_region_per_30secs AS
  SELECT regionid,
         COUNT(*)
  FROM pageviews
  WINDOW TUMBLING (SIZE 30 SECONDS)
  WHERE UCASE(gender)='FEMALE' AND LCASE(regionid)='region_6'
  GROUP BY regionid
  EMIT CHANGES;
```

### Aggregate Records Over a Hopping Window

This query computes the count for a hopping window of 30 seconds that
advances by 10 seconds.

UCASE and LCASE functions are used to convert the values of `gender` and
`regionid` columns to uppercase and lowercase, so that you can match
them correctly. ksqlDB also supports the LIKE operator for prefix, suffix,
and substring matching.

```sql
CREATE TABLE pageviews_per_region_per_30secs10secs AS
  SELECT regionid,
         COUNT(*)
  FROM pageviews
  WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 10 SECONDS)
  WHERE UCASE(gender)='FEMALE' AND LCASE (regionid) LIKE '%_6'
  GROUP BY regionid
  EMIT CHANGES;
```

### Aggregate Records Over a Session Window

The following query counts the number of pageviews per region for
session windows, with a session inactivity gap of 60 seconds. This query
*sessionizes* the input data and performs the counting step per region.

```sql
CREATE TABLE pageviews_per_region_per_session AS
  SELECT regionid,
         COUNT(*)
  FROM pageviews
  WINDOW SESSION (60 SECONDS)
  GROUP BY regionid
  EMIT CHANGES;
```

For more information, see
[Time and Windows in ksqlDB](../concepts/time-and-windows-in-ksqldb-queries.md).

Next Steps
----------

-   Watch the screencast of [Aggregations](https://www.youtube.com/embed/db5SsmNvej4).
-   [Aggregating Data](https://www.confluent.io/stream-processing-cookbook/ksql-recipes/aggregating-data)
-   [Detecting Abnormal Transactions](https://www.confluent.io/stream-processing-cookbook/ksql-recipes/detecting-abnormal-transactions)
-   [Inline Streaming Aggregation](https://www.confluent.io/stream-processing-cookbook/ksql-recipes/inline-streaming-aggregation)

Page last revised on: {{ git_revision_date }}
