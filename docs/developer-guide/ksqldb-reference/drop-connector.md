---
layout: page
title: DROP CONNECTOR
tagline:  ksqlDB DROP CONNECTOR statement
description: Syntax for the DROP CONNECTOR statement in ksqlDB
keywords: ksqlDB, drop, connector, connect
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/drop-connector.html';
</script>

DROP CONNECTOR
==============

Synopsis
--------

```sql
DROP CONNECTOR [IF EXISTS] connector_name;
```

Description
-----------

Drop a connector and delete it from the {{ site.kconnect }} cluster. The
topics associated with this cluster are not deleted by this command.

If the IF EXISTS clause is present, the statement doesn't fail if the
connector doesn't exist.