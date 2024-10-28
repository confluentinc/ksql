---
layout: page
title: RESUME
tagline:  ksqlDB RESUME statement
description: Syntax for the RESUME statement in ksqlDB
keywords: ksqlDB, query, resume
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/resume.html';
</script>

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
