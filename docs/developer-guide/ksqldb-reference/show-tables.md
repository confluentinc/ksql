---
layout: page
title: SHOW STREAMS
tagline:  ksqlDB SHOW STREAMS statement
description: Syntax for the SHOW STREAMS statement in ksqlDB
keywords: ksqlDB, list, stream
---

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

