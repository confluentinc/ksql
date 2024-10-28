---
layout: page
title: PAUSE
tagline:  ksqlDB PAUSE statement
description: Syntax for the PAUSE statement in ksqlDB
keywords: ksqlDB, query, pause
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/pause.html';
</script>

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
