---
layout: page
title: DROP TYPE
tagline:  ksqlDB DROP TYPE statement
description: Syntax for the DROP TYPE statement in ksqlDB
keywords: ksqlDB, drop, type, alias, struct
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/drop-type.html';
</script>

DROP TYPE
=========

Synopsis
--------

```sql
DROP TYPE [IF EXISTS] <type_name> AS <type>;
```

Description
-----------

Removes a type alias from ksqlDB. This statement doesn't fail if the type is in
use in active queries or user-defined functions, because the DROP TYPE
statement doesn't track whether queries are using the type. This means that you
can drop a type any time, and old queries continue to work. Also, old queries
running with a dropped type and don't change if you register a new type with
the same name.

If the IF EXISTS clause is present, the statement doesn't fail if the
type doesn't exist.

Example
-------

```sql
DROP TYPE ADDRESS;
```

