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
TERMINATE query_id;
```

Description
-----------

Terminate a persistent query. Persistent queries run continuously until
they are explicitly terminated.

-   In client-server mode, exiting the CLI doesn't stop persistent
    queries, because the ksqlDB Server(s) continue to process the
    queries.

To terminate a non-persistent query, use Ctrl+C in the CLI.

Example
-------

TODO: example

Page last revised on: {{ git_revision_date }}
