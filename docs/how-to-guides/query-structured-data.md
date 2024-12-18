---
layout: page
title: How to query structured data 
tagline: Query structs, maps, and arrays by using ksqlDB
description: Read the inner contents of structs, maps, and arrays in ksqlDB queries 
keywords: query, structured data, struct, array, map
---

# How to query structured data

## Context

You have events that contain structured data types like structs, maps, and arrays. You want to write them to ksqlDB and read their inner contents with queries. Because ksqlDB represents each event as a row with a flat series of columns, you need a bit of syntax to work with these data types. This is sometimes called "destructuring".

## In action

```sql
SELECT a->d    AS d,   -- destructure a struct
       b[1]    AS b_1  -- destructure an array
       c['k1'] AS k1   -- destructure a map
FROM s1
EMIT CHANGES;
```

## Data types

### Structs

Structs are an associative data type that map `VARCHAR` keys to values of any type. Destructure structs by using arrow syntax (`->`).

Begin by telling ksqlDB to start all queries from the earliest point in each topic.

```sql
SET 'auto.offset.reset' = 'earliest';
```

Make a stream `s2` with two columns: `a` and `b`. `b` is a struct with `VARCHAR` keys `c` and `d`, whose value data types are `VARCHAR` and `INT` respectively.

```sql
CREATE STREAM s2 (
    a VARCHAR KEY,
    b STRUCT<
        c VARCHAR,
        d INT
    >
) WITH (
    kafka_topic = 's2',
    partitions = 1,
    value_format = 'avro'
);
```

Insert some rows into `s2`. You can represent a struct literal by using the `STRUCT` constructor, which takes a variable number of key/value arguments.

```sql
INSERT INTO s2 (
    a, b
) VALUES (
    'k1', STRUCT(c := 'v1', d := 5)
);

INSERT INTO s2 (
    a, b
) VALUES (
    'k2', STRUCT(c := 'v2', d := 6)
);

INSERT INTO s2 (
    a, b
) VALUES (
    'k3', STRUCT(c := 'v3', d := 7)
);
```

To access a struct in a query, start with the name of a column and add `->` each time you want to drill into a key. This query selects column `a`, `b`, the key `c` within `b`, and the key `d` within `b`:

```sql
SELECT a,
       b,
       b->c,
       b->d
FROM s2
EMIT CHANGES;
```

Starting in ksqlDB 0.27, you can access all fields of a struct by using the `->*` after the column. For instance, this query behaves similar to the previous query by selecting columns `a`, `b`, and all keys withing `b`:
```sql
SELECT a,
       b,
       b->*
FROM s2
EMIT CHANGES;
```

Your output should resemble the following results. Notice that the column names for the last two columns are `C` and `D` respectively. By default, ksqlDB will give the column the name of the last identifier in the arrow chain. You can override this by aliasing, such as `b->c AS x`. If you drill into nested values that finish with the same identifier name, ksqlDB will force you to provide an alias to avoid ambiguity.

```
+------------------------------+------------------------------+------------------------------+------------------------------+
|A                             |B                             |C                             |D                             |
+------------------------------+------------------------------+------------------------------+------------------------------+
|k1                            |{C=v1, D=5}                   |v1                            |5                             |
|k2                            |{C=v2, D=6}                   |v2                            |6                             |
|k3                            |{C=v3, D=7}                   |v3                            |7                             |
```


### Maps

Maps are an associative data type that map keys of any type to values of any type. The types across all keys must be the same. The same rule holds for values. Destructure maps using bracket syntax (`[]`).

Begin by telling ksqlDB to start all queries from the earliest point in each topic.

```sql
SET 'auto.offset.reset' = 'earliest';
```

Make a stream `s3` with two columns: `a` and `b`. `b` is a map with `VARCHAR` keys and `INT` values.

```sql
CREATE STREAM s3 (
    a VARCHAR KEY,
    b MAP<VARCHAR, INT>
) WITH (
    kafka_topic = 's3',
    partitions = 1,
    value_format = 'avro'
);
```

Insert some rows into `s3`. You can represent a MAP literal by using the `MAP` constructor, which takes a variable number of key/value arguments. `c` and `d` are used consistently in this example, but the key names can be heterogeneous in practice.

```sql
INSERT INTO s3 (
    a, b
) VALUES (
    'k1', MAP('c' := 2, 'd' := 4)
);

INSERT INTO s3 (
    a, b
) VALUES (
    'k2', MAP('c' := 4, 'd' := 8)
);

INSERT INTO s3 (
    a, b
) VALUES (
    'k3', MAP('c' := 8, 'd' := 16)
);
```

To access a map in a query, start with the name of a column and add `[]` each time you want to drill into a key. This query selects column `a`, `b`, the key `c` within `b`, and the key `d` within `b`:

```sql
SELECT a,
       b,
       b['c'] AS C,
       b['d'] AS D
FROM s3
EMIT CHANGES;
```

This query should return the following results. The last two column names have been aliased. If you elect not to give them a name, ksqlDB will generate names like `KSQL_COL_0` for each.

```
+------------------------------+------------------------------+------------------------------+------------------------------+
|A                             |B                             |C                             |D                             |
+------------------------------+------------------------------+------------------------------+------------------------------+
|k1                            |{c=2, d=4}                    |2                             |4                             |
|k2                            |{c=4, d=8}                    |4                             |8                             |
|k3                            |{c=8, d=16}                   |8                             |16                            |
```

### Arrays

Arrays are a collection data type that contain a sequence of values of a single type. Destructure arrays using bracket syntax (`[]`).

Begin by telling ksqlDB to start all queries from the earliest point in each topic.

```sql
SET 'auto.offset.reset' = 'earliest';
```

Make a stream `s4` with two columns: `a` and `b`. `b` is an array with `INT` elements.

```sql
CREATE STREAM s4 (
    a VARCHAR KEY,
    b ARRAY<INT>
) WITH (
    kafka_topic = 's4',
    partitions = 1,
    value_format = 'avro'
);
```

Insert some rows into `s4`. You can represent an array literal by using the `ARRAY` constructor, which takes a variable number of elements.

```sql
INSERT INTO s4 (
    a, b
) VALUES (
    'k1', ARRAY[1]
);

INSERT INTO s4 (
    a, b
) VALUES (
    'k2', ARRAY[2, 3]
);

INSERT INTO s4 (
    a, b
) VALUES (
    'k3', ARRAY[4, 5, 6]
);
```

To access an array in a query, start with the name of a column and add `[]` each index you want to drill into. This query selects column `a`, `b`, the first element of `b`, the second element of `b`, the third element of `b`, and the last element of `b`:

```sql
SELECT a,
       b,
       b[1] AS b_1,
       b[2] AS b_2,
       b[3] AS b_3,
       b[-1] AS b_minus_1
FROM s4
EMIT CHANGES;
```

This query should return the following results. Notice that index `1` represents the first element of each array. By contrast to many programming languages which represent the first element of an array as `0`, most databases, like ksqlDB, represent it as `1`. If an element is absent, the result is `null`. You can use negative indices to navigate backwards through the array. In this example, `-1` retrieves the last element of each array regardless of its length.

```
+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
|A                  |B                  |B_1                |B_2                |B_3                |B_MINUS_1          |
+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
|k1                 |[1]                |1                  |null               |null               |1                  |
|k2                 |[2, 3]             |2                  |3                  |null               |3                  |
|k3                 |[4, 5, 6]          |4                  |5                  |6                  |6                  |
```

## Deeply nested data

You may have structured data types that are nested within one another. Each data type's destructuring syntax composes irrespective of how it is nested.

Begin by telling ksqlDB to start all queries from the earliest point in each topic.

```sql
SET 'auto.offset.reset' = 'earliest';
```

Make a stream `s4` with two columns: `a` and `b`. Here is how `b` breaks down:

- `b` is a struct with `VARCHAR` keys `c` and `d`.
- `c` is an array of `INT` elements.
- `d` is a map of `VARCHAR` keys and struct values.
- That struct has keys `e` and `f`, with values of type `VARCHAR` and `BOOLEAN` respectively.

```sql
CREATE STREAM s4 (
    a VARCHAR KEY,
    b STRUCT<
        c ARRAY<INT>,
        d MAP<
            VARCHAR,
            STRUCT<
                e VARCHAR,
                f BOOLEAN
            >
        >
    >
) WITH (
    kafka_topic = 's4',
    partitions = 1,
    value_format = 'avro'
);
```

Insert some rows into `s4`. Notice how the constructors for each data type readily compose.

```sql
INSERT INTO s4 (
    a, b
) VALUES (
    'k1',
    STRUCT(
        c := ARRAY[5, 10, 15],
        d := MAP(
            'x' := STRUCT(e := 'v1', f := true),
            'y' := STRUCT(e := 'v2', f := false)
        )
    )
);

INSERT INTO s4 (
    a, b
) VALUES (
    'k2',
    STRUCT(
        c := ARRAY[3, 6, 9],
        d := MAP(
            'x' := STRUCT(e := 'v3', f := false),
            'y' := STRUCT(e := 'v4', f := false)
        )
    )
);

INSERT INTO s4 (
    a, b
) VALUES (
    'k3',
    STRUCT(
        c := ARRAY[2, 4, 8],
        d := MAP(
            'x' := STRUCT(e := 'v5', f := true),
            'y' := STRUCT(e := 'v6', f := true)
        )
    )
);
```

To access nested values, use the destructuring syntax from each data type. Notice how you can chain them together:

```sql
SELECT a,
       b,
       b->c[2] AS c_2,
       b->d['x']->f,
       b->d['y']->e
FROM s4
EMIT CHANGES;
```

This query should return the following results. The rules for how each column name is generated are based on the data type that is at the tail of each selected element.

```
+--------------------------------------------+--------------------------------------------+--------------------------------------------+--------------------------------------------+--------------------------------------------+
|A                                           |B                                           |C_2                                         |F                                           |E                                           |
+--------------------------------------------+--------------------------------------------+--------------------------------------------+--------------------------------------------+--------------------------------------------+
|k1                                          |{C=[5, 10, 15], D={x={E=v1, F=true}, y={E=v2|10                                          |true                                        |v2                                          |
|                                            |, F=false}}}                                |                                            |                                            |                                            |
|k2                                          |{C=[3, 6, 9], D={x={E=v3, F=false}, y={E=v4,|6                                           |false                                       |v4                                          |
|                                            | F=false}}}                                 |                                            |                                            |                                            |
|k3                                          |{C=[2, 4, 8], D={x={E=v5, F=true}, y={E=v6, |4                                           |true                                        |v6                                          |
|                                            |F=true}}}                                   |                                            |                                            |                                            |
```
