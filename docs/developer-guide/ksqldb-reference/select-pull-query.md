---
layout: page
title: SELECT (Pull Query)
tagline:  ksqlDB SELECT statement for pull queries
description: Syntax for the SELECT statement in ksqlDB for pull queries
keywords: ksqlDB, select, pull query
---

SELECT (Pull Query)
===================

Synopsis
--------

```sql
SELECT select_expr [, ...]
  FROM aggregate_table
  WHERE key_column=key
  [AND window_bounds];
```

Description
-----------

Pulls the current value from the materialized table and terminates. The result
of this statement isn't persisted in a {{ site.ak }} topic and is printed out
only in the console.

Pull queries enable you to fetch the current state of a materialized view.
Because materialized views are incrementally updated as new events arrive,
pull queries run with predictably low latency. They're a great match for
request/response flows. For asynchronous application flows, see
[Push Queries](select-push-query.md).

Execute a pull query by sending an HTTP request to the ksqlDB REST API, and
the API responds with a single response.  

The WHERE clause must contain a single primary-key to retrieve and may
optionally include bounds on `WINDOWSTART` and `WINDOWEND` if the materialized table is windowed.
For more information, see 
[Time and Windows in ksqlDB](../../concepts/time-and-windows-in-ksqldb-queries.md).

Example
-------

```sql
SELECT * FROM pageviews_by_region
  WHERE regionId = 'Region_1'
    AND 1570051876000 <= WINDOWSTART AND WINDOWEND <= 1570138276000;
```

When writing logical expressions using `WINDOWSTART` or `WINDOWEND`, you can use ISO-8601
formatted datestrings to represent date times. For example, the previous
query is equivalent to the following:

```sql
SELECT * FROM pageviews_by_region
  WHERE regionId = 'Region_1'
    AND '2019-10-02T21:31:16' <= WINDOWSTART AND WINDOWEND <= '2019-10-03T21:31:16';
```

You can specify time zones within the datestring. For example,
`2017-11-17T04:53:45-0330` is in the Newfoundland time zone. If no time zone is
specified within the datestring, then timestamps are interpreted in the UTC
time zone.

If no bounds are placed on `WINDOWSTART` or `WINDOWEND`, rows are returned for all windows
in the windowed table.
