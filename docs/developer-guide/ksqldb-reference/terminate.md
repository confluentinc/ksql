---
layout: page
title: TERMINATE
tagline:  ksqlDB TERMINATE statement
description: Syntax for the TERMINATE statement in ksqlDB
keywords: ksqlDB, query, end, stop
---

TERMINATE
=========

Synopsis
--------

```sql
TERMINATE query_id | ALL;
```

Description
-----------

Terminate a query. Persistent queries run continuously until
they are explicitly terminated.

-   In client-server mode, exiting the CLI doesn't stop persistent
    queries, because the ksqlDB Server(s) continue to process the
    queries.

A non-persistent query can also be terminated by using Ctrl+C in the CLI.
