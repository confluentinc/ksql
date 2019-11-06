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
  SELECT select_expr [., ...]
  FROM from_stream
  [ WHERE condition ]
  [ PARTITION BY column_name ]
  EMIT CHANGES;
```

Description
-----------

Stream the result of the SELECT query into an existing stream and its
underlying topic.

The schema and partitioning column produced by the query must match the
stream's schema and key, respectively. If the schema and partitioning
column are incompatible with the stream, then the statement will return
an error.

The `stream_name` and `from_item` parameters must both refer to a Stream.
Tables are not supported.

Records written into the stream are not timestamp-ordered with respect
to other queries. Therefore, the topic partitions of the output stream
may contain out-of-order records even if the source stream for the query
is ordered by timestamp.

Example
-------

TODO: example

Page last revised on: {{ git_revision_date }}
