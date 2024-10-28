---
layout: page
title: SHOW STREAMS
tagline:  ksqlDB SHOW STREAMS statement
description: Syntax for the SHOW STREAMS statement in ksqlDB
keywords: ksqlDB, list, stream
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/show-tables.html';
</script>

SHOW TABLES
===========

Synopsis
--------

```sql
SHOW | LIST TABLES [EXTENDED];
```

Description
-----------

List the defined tables.

Example
-------


```sql
-- See the list of tables currently registered:
LIST TABLES;

-- See extended information about currently registered tables:
SHOW TABLES EXTENDED; 
```

