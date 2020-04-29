---
layout: page
title: DROP CONNECTOR
tagline:  ksqlDB DROP CONNECTOR statement
description: Syntax for the DROP CONNECTOR statement in ksqlDB
keywords: ksqlDB, drop, connector, connect
---

DROP CONNECTOR
==============

Synopsis
--------

```sql
DROP CONNECTOR connector_name;
```

Description
-----------

Drop a connector and delete it from the {{ site.kconnect }} cluster. The
topics associated with this cluster are not deleted by this command.

Page last revised on: {{ git_revision_date }}