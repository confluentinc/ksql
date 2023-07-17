---
layout: page
title: TERMINATE
tagline:  ksqlDB TERMINATE statement
description: Syntax for the TERMINATE statement in ksqlDB
keywords: ksqlDB, query, end, stop
---

RUN SCRIPT
==========

Synopsis
--------

```sql
RUN SCRIPT <path-to-query-file>;
```

Description
-----------

You can run a list of predefined queries and commands from in a file by
using the RUN SCRIPT command.

The RUN SCRIPT command supports a subset of ksqlDB statements:

-   Persistent queries: [CREATE STREAM](create-stream.md),
    [CREATE TABLE](create-table.md), [CREATE STREAM AS SELECT](create-stream-as-select.md),
    [CREATE TABLE AS SELECT](create-table-as-select.md)
-   [DROP STREAM](drop-stream.md) and [DROP TABLE](drop-table.md)
-   SET, UNSET statements
-   INSERT INTO statement

The RUN SCRIPT doesn't support statements such as:

-   SHOW TOPICS and SHOW STREAMS, *etc*.
-   TERMINATE
-   Non-persistent queries: SELECT, *etc*.

RUN SCRIPT can also be used from the command line, for instance when
writing shell scripts.

Example
-------

The following statement runs the queries in the file located at
`/local/path/to/queries.sql`.

```sql
RUN SCRIPT '/local/path/to/queries.sql';
```

