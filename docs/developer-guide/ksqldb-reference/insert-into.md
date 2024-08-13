---
layout: page
title: INSERT INTO
tagline:  ksqlDB INSERT INTO statement
description: Syntax for the INSERT INTO statement in ksqlDB
keywords: ksqlDB, insert
---

INSERT INTO
===========

Synopsis
--------

```sql
INSERT INTO stream_name
  WITH ( property_name = expression [, ...] )
  SELECT select_expr [, ...]
  FROM from_stream
  [ LEFT | FULL | INNER ]
      JOIN [join_table | join_stream]
        [WITHIN [<size> <timeunit> | (<before_size> <timeunit>, <after_size> <timeunit>)] [GRACE PERIOD <grace_size> <timeunit>]]
      ON join_criteria
  [ WHERE condition ]
  [ PARTITION BY new_key_expr [, ...] ]
  EMIT CHANGES;
```

Description
-----------

Stream the result of the SELECT query into an existing stream and its
underlying topic.

The schema and partitioning column(s) produced by the query must match the
stream's schema and key, respectively. If the schema and partitioning
column(s) are incompatible with the stream, then the statement will return
an error.

The `stream_name` and `from_stream` parameters must both refer to a Stream.
Tables are not supported.

If the PARTITION BY clause is present, it is applied to the source _after_ any JOIN or WHERE clauses, and
_before_ the SELECT clause, in much the same way as GROUP BY.

Records written into the stream are not timestamp-ordered with respect
to other queries. Therefore, the topic partitions of the output stream
may contain out-of-order records even if the source stream for the query
is ordered by timestamp.

## Insert properties

Use the `WITH` clause to specify details about your table. The `WITH` clause
supports the following properties.

### QUERY_ID

Optional query ID to use for INSERT INTO queries. This query ID will be 
displayed by `SHOW QUERIES`, and will also be used to terminate the query.
If empty, the default 'INSERTQUERY_' prefix plus a number will be used. 
Queries IDs are case-insensitive. Queries IDs will be displayed in uppercase with `SHOW QUERIES`
