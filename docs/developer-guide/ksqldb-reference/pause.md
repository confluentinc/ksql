---
layout: page
title: PAUSE
tagline:  ksqlDB PAUSE statement
description: Syntax for the PAUSE statement in ksqlDB
keywords: ksqlDB, query, pause
---

PAUSE
=========

Synopsis
--------

```sql
PAUSE query_id | ALL;
```

Description
-----------

Pause a persistent query.  Transient queries cannot be paused or resumed.
