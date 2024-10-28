---
layout: page
title: SPOOL
tagline:  ksqlDB SPOOL statement
description: Syntax for the SPOOL statement in ksqlDB
keywords: ksqlDB, command, file
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/spool.html';
</script>

SPOOL
=====

Synopsis
--------

```sql
SPOOL <file_name|OFF>
```

Description
-----------

Stores issued commands and their results into a file. Only one spool may
be active at a time and can be closed by issuing `SPOOL OFF` . Commands
are prefixed with `ksql>` to differentiate from output.
