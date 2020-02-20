---
layout: page
title: SHOW QUERIES
tagline:  ksqlDB SHOW QUERIES statement
description: Syntax for the SHOW QUERIES statement in ksqlDB
keywords: ksqlDB, list, query
---

SHOW QUERIES
============

Synopsis
--------

```sql
SHOW | LIST [ALL] QUERIES [EXTENDED];
```

Description
-----------

List the running persistent queries.

`SHOW QUERIES` lists queries running on the node that receives the request

`SHOW QUERIES EXTENDED` Lists queries running on the node that receives the request in more detail

`SHOW ALL QUERIES` lists all queries running across all nodes in the cluster

Example
-------

TODO: example

Page last revised on: {{ git_revision_date }}
