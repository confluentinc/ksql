---
layout: page
title: ALTER SYSTEM
tagline:  ksqlDB ALTER SYSTEM statement
description: Syntax for the ALTER SYSTEM statement in ksqlDB
keywords: ksqlDB, system, config, properties
---

ALTER SYSTEM
==========

Synopsis
--------

```sql
ALTER SYSTEM '<config-name>'='<value>';
```

Description
-----------

Sets and modifies the value of system-level configs. These
are properties that are applied to all queries in the cluster. Using `ALTER SYSTEM` 
results in the new value being applied immediately across the cluster.

`ALTER SYSTEM` is only available from the ksql command line and only when connected to a
ksqlDB cluster in {{ site.ccloud }}.

Example
-------

The following statement sets the streams property `auto.offset.reset` to begin at the
`earliest` record in the input topics when no committed offsets are found.

```sql
ALTER SYSTEM 'auto.offset.reset'='earliest';
```

