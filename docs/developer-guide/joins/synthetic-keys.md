---
layout: page
title: Join Synthetic Key Columns
tagline: Understanding synthetic key columns
description: Learn which joins result in synthetic key columns and how to work with them.
keywords: ksqldb, join, key, rowkey
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/joins/synthetic-keys.html';
</script>

Some joins have a synthetic key column in their result. This is a column that does not come from any
source. Here's an example to help explain what synthetic key columns are and why they are required:

```sql
CREATE TABLE OUTPUT AS
  SELECT * FROM L FULL OUTER JOIN R ON L.ID = R.ID;
```

The previous statement seems straightforward enough: create a new table that's the result of
performing a full outer join of two source tables, joining on their ID columns. But in a 
full-outer join, either `L.ID` or `R.ID` may be missing (`NULL`), or both 
may have the same value. Since the data produced to {{ site.aktm }} should always have a non-null 
record key, ksql selects the first non-null key to use:

| L.ID  | R.ID | Kafka record key |
|-------|------|:------------------|
|  10   | null | 10                |
|  null | 7    | 7                 |
|  8    | 8    | 8                 |

The data stored in the {{ site.ak }} record's key may not match either of the source `ID`
columns. Instead, it's a new column: a *synthetic* column, which means a column that doesn't belong
to either source table.

## Which joins result in synthetic key columns?

Any join where the key column in the result does not match any source column is said to have a 
synthetic key column.

The following types of joins result in a synthetic key column being added to the result schema:

1. `FULL OUTER` joins, for example:

    ```sql
   CREATE TABLE OUTPUT AS
      SELECT * FROM L FULL OUTER JOIN R ON L.ID = R.ID;
    ```

 
2. Any join where all expressions used in the join `ON` criteria are not simple column references.
   For example: 

    ```sql
   -- join on expressions other than column references:
   CREATE TABLE OUTPUT AS
      SELECT * FROM L JOIN R ON ABS(L.ID) = ABS(R.ID);
    ```

## What name is assigned to a Synthetic key column?

The default name of a synthetic key column is `ROWKEY`. But, if any sources used in the join 
already contain a column named `ROWKEY`, the synthetic key column is named `ROWKEY_1`, or
`ROWKEY_2` if there exists a source column called `ROWKEY_1`, etc. For example: 

```sql
-- given sources:
CREATE STREAM S1 (ROWKEY INT KEY, V0 STRING) WITH (...);
CREATE STREAM S2 (ID INT KEY, ROWKEY_1 INT) WITH (...);

CREATE STREAM OUTPUT AS
  SELECT * 
  FROM S1 JOIN S2 
  WITHIN 30 SECONDS 
  ON ABS(S1.ROWKEY) = ABS(S2.ID);

-- result in OUTPUT with synthetic key column name: ROWKEY_2
```

Like any other key column, the synthetic key column must be included in the projection of streaming
queries. If your projection is missing the synthetic key, then an error like the one below will be
returned, indicating the name of the missing key column:

```
Key missing from projection.
The query used to build `OUTPUT` must include the join expression ROWKEY in its projection.
ROWKEY was added as a synthetic key column because the join criteria did not match any source column. This expression must be included in the projection and may be aliased. 
```  

Optionally, you may provide an alias for the key column in the projection. This is recommended, as 
system generated names are not guaranteed to remain consistent between versions. For example: 

```sql
CREATE STREAM OUTPUT AS
   SELECT ROWKEY AS ID, S1.C0, S2.C1 FROM S1 FULL OUTER JOIN S2 ON S1.ID = S2.ID;
```

## Suggested Reading

- Blog post: [I’ve Got the Key, I’ve Got the Secret. Here’s How Keys Work in ksqlDB 0.10](https://www.confluent.io/blog/ksqldb-0-10-updates-key-columns/)
