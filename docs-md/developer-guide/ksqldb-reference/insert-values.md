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

If no columns are specified, a value for every column is expected in the same order as
the schema. If columns are specified, the order of values must match the order of the names.
Any column not explicitly given a value is set to `null`.

!!! note
	`ROWTIME` may be specified as an explicit column but isn't required
    when you omit the column specifications.

!!! note
  While streams will allow inserts where the KEY column is `null`, tables require a value for their
  PRIMARY KEY and will reject any statement without one.

Example
-------

The following statements are valid for a source with a schema like
`ID INT KEY, COL_A VARCHAR`.

```sql
-- inserts (rowtime=1234, id="key", col_a="A")
INSERT INTO foo (ROWTIME, ID, COL_A) VALUES (1234, 'key', 'A');

-- also inserts (rowtime=1234, id="key", col_a="A")
INSERT INTO foo (COL_A, ID, ROWTIME) VALUES ('A', 'key', 1234);

-- inserts (rowtime=current_time(), id="key", col_a="A")
INSERT INTO foo VALUES ('key', 'A');

-- inserts (rowtime=current_time(), id="key", COL_A="A")
INSERT INTO foo (KEY_COL, COL_A) VALUES ('key', 'A');

-- inserts (rowtime=current_time(), id="key", COL_A=null)
INSERT INTO foo (KEY_COL) VALUES ('key');
```

Page last revised on: {{ git_revision_date }}
