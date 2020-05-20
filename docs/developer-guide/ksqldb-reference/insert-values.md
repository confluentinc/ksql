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
`<KEY_COL VARCHAR, COL_A VARCHAR>` with `KEY=KEY_COL`.

```sql
-- inserts (1234, "key", "key", "A")
INSERT INTO foo (ROWTIME, ROWKEY, KEY_COL, COL_A) VALUES (1510923225000, 'key', 'key', 'A');

-- inserts (current_time(), "key", "key", "A")
INSERT INTO foo VALUES ('key', 'key', 'A');

-- inserts (current_time(), "key", "key", "A")
INSERT INTO foo (KEY_COL, COL_A) VALUES ('key', 'A');

-- inserts (current_time(), "key", "key", null)
INSERT INTO foo (KEY_COL) VALUES ('key');
```

The values are serialized by using the `value_format` specified in the
original `CREATE` statement. The key is always serialized as a String.
