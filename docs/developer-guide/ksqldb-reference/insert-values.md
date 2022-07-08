---
layout: page
title: INSERT VALUES
tagline:  ksqlDB INSERT VALUES statement
description: Syntax for the INSERT VALUES statement in ksqlDB
keywords: ksqlDB, insert value
---

INSERT VALUES
=============

Synopsis
--------

```sql
INSERT INTO <stream_name|table_name> [(column_name [, ...]])]
  VALUES (value [,...]);
```

Description
-----------

Produce a row into an existing stream or table and its underlying topic
based on explicitly specified values.

If column names are specified, the order of the values must match the
order of the names. Any column not explicitly given a value is set to `null`.
If a column is given a null value or is not set, it does not act as a tombstone. 

Tombstones are not supported with `INSERT INTO`.
Pseudo columns, for example `ROWTIME`, may be provided.

If no columns are specified, a value for every column is expected in the same
order as the schema, with key columns first.

!!! note
	`ROWTIME` may be specified as an explicit column but isn't required
   when you omit the column specifications. If not supplied, it defaults
   to the local machine time.

Example
-------

The following statements are valid for a source with a schema like
`KEY_COL VARCHAR KEY, COL_A VARCHAR`.

```sql
-- inserts (ROWTIME:=1510923225000, KEY_COL:="key", COL_A:="A")
INSERT INTO foo (ROWTIME, KEY_COL, COL_A) VALUES (1510923225000, 'key', 'A');

-- also inserts (ROWTIME:=1510923225000, KEY_COL:="key", COL_A:="A")
INSERT INTO foo (COL_A, ROWTIME, KEY_COL) VALUES ('A', 1510923225000, 'key');

-- inserts (ROWTIME:=current_time(), KEY_COL:="key", COL_A:="A")
INSERT INTO foo VALUES ('key', 'A');

-- inserts (ROWTIME:=current_time(), KEY_COL:="key", COL_A:="A")
INSERT INTO foo (KEY_COL, COL_A) VALUES ('key', 'A');

-- inserts (ROWTIME:=current_time(), KEY_COL:="key", COL_A:=null)
INSERT INTO foo (KEY_COL) VALUES ('key');
```

The values are serialized by using the format(s) specified in the original
`CREATE` statement.
