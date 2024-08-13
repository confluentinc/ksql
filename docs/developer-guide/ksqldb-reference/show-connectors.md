---
layout: page
title: SHOW CONNECTORS
tagline:  ksqlDB SHOW CONNECTORS statement
description: Syntax for the SHOW CONNECTORS statement in ksqlDB
keywords: ksqlDB, show, list, connector, connect
---

SHOW CONNECTORS
===============

Synopsis
--------

```sql
SHOW | LIST [SOURCE | SINK] CONNECTORS;
```

Description
-----------

List all connectors in the {{ site.kconnect }} cluster.

!!! note
	The SHOW and LIST statements don't distinguish connectors that are created by
    using the ksqlDB from connectors that are created independently by using the
    {{ site.kconnect }} API.

