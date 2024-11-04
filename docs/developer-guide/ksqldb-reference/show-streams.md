---
layout: page
title: SHOW STREAMS
tagline:  ksqlDB SHOW STREAMS statement
description: Syntax for the SHOW STREAMS statement in ksqlDB
keywords: ksqlDB, list, stream
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/show-streams.html';
</script>

SHOW STREAMS
============

Synopsis
--------

```sql
SHOW | LIST STREAMS [EXTENDED];
```

Description
-----------

List the defined streams.

Example
-------

```sql
-- See the list of streams currently registered:
SHOW STREAMS;

-- See extended information about currently registered streams:
LIST STREAMS EXTENDED; 
```

