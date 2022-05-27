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

You can use this command to set and modify the value of system-level configs. These
are properties that are applied to all queries in the cluster, and using `ALTER SYSTEM` 
will result in the new value immediately being applied across the cluster.

`ALTER SYSTEM` is only available from the ksql command line and only when connected to a
ksql cluster in CCloud.

Example
-------

The following statement sets the streams property `auto.offset.reset` to pick up from
`earliest` in the input topics when no committed offsets are found.

```sql
ALTER SYSTEM 'auto.offset.reset'='earliest';
```

