---
layout: page
title: RESUME
tagline:  ksqlDB RESUME statement
description: Syntax for the RESUME statement in ksqlDB
keywords: ksqlDB, query, resume
---

RESUME
=========

Synopsis
--------

```sql
RESUME query_id | ALL;
```

Description
-----------

Resume a paused persistent query.  Transient queries cannot be paused or resumed.
