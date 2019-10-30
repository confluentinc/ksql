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
based on explicitly specified values. The first `column_name` of every
schema is `ROWKEY`, which defines the corresponding Kafka key. If the
source specifies a `key` and that column is present in the column names
for this INSERT statement then that value and the `ROWKEY` value are
expected to match, otherwise the value from `ROWKEY` will be copied into
the value of the key column (or conversely from the key column into the
`ROWKEY` column).

Any column not explicitly given a value is set to `null`. If no columns
are specified, a value for every column is expected in the same order as
the schema with `ROWKEY` as the first column. If columns are specified,
the order does not matter.

!!! note
	`ROWTIME` may be specified as an explicit column but isn't required
    when you omit the column specifications.

Example
-------

The following statements are valid for a source with a schema like
`<KEY_COL VARCHAR, COL_A VARCHAR>` with `KEY=KEY_COL`.

```sql
-- inserts (1234, "key", "key", "A")
INSERT INTO foo (ROWTIME, ROWKEY, KEY_COL, COL_A) VALUES (1234, 'key', 'key', 'A');

-- inserts (current_time(), "key", "key", "A")
INSERT INTO foo VALUES ('key', 'key', 'A');

-- inserts (current_time(), "key", "key", "A")
INSERT INTO foo (KEY_COL, COL_A) VALUES ('key', 'A');

-- inserts (current_time(), "key", "key", null)
INSERT INTO foo (KEY_COL) VALUES ('key');
```

The values are serialized by using the `value_format` specified in the
original `CREATE` statement. The key is always serialized as a String.