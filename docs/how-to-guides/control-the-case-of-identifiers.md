---
layout: page
title: How to control the case of identifiers 
tagline: Casing for ksqlDB stream name and column names
description: Use backticks to control the casing of ksqlDB stream name and column names
keywords: case, identifier
---

# How to control the case of identifiers

## Context

You have identifiers, like row names, that will be used outside of
ksqlDB. You want to control the exact casing (capitalization) of how
they are represented to make them consumable by downstream
programs. Because ksqlDB uppercases all identifiers by default, you
need to use backticks to preserve the desired casing.

## In action

```sql
CREATE STREAM `s1` (
    `foo` VARCHAR KEY,
    `BAR` INT,
    `Baz` VARCHAR
) WITH (
    kafka_topic = 's1',
    partitions = 1,
    value_format = 'avro'
);
```

## Backticks

Begin by telling ksqlDB to start all queries from the earliest point
in each topic.

```sql
SET 'auto.offset.reset' = 'earliest';
```

Declare a new stream named `s2`. In this example, you override
ksqlDB's default behavior to uppercase all identifiers. Use backticks
to control the casing of the stream name and column names. To contrast
this behavior, `qux` is declared without backticks to demonstrate
the default behavior of uppercasing.

```sql
CREATE STREAM `s2` (
    `foo` VARCHAR KEY,
    `BAR` INT,
    `Baz` VARCHAR,
    `grault` STRUCT<
        `Corge` VARCHAR,
        `garply` INT
    >,
    qux INT
) WITH (
    kafka_topic = 's2',
    partitions = 1,
    value_format = 'avro'
);
```

Insert some rows into `s2`. Notice how you need to use backticks each
time to reference a field that doesn't have the default casing:

```sql
INSERT INTO `s2` (
    `foo`, `BAR`, `Baz`, `grault`, qux
) VALUES (
    'k1', 1, 'x', STRUCT(`Corge` := 'v1', `garply` := 5), 2
);

INSERT INTO `s2` (
    `foo`, `BAR`, `Baz`, `grault`, qux
) VALUES (
    'k2', 3, 'y', STRUCT(`Corge` := 'v2', `garply` := 6), 4
);

INSERT INTO `s2` (
    `foo`, `BAR`, `Baz`, `grault`, qux
) VALUES (
    'k3', 5, 'z', STRUCT(`Corge` := 'v3', `garply` := 8), 6
);
```

Issue a push query to select the rows. The relevant identifiers are
again surrounded with backticks. Notice that since `qux` wasn't
declared with backticks, it can be referred to with any casing. ksqlDB
will find the matching identifier by uppercasing it automatically.

```sql
SELECT `foo`,
       `BAR`,
       `Baz`,
       `grault`->`Corge`,
       `grault`->`garply`,
       qux,
       QUX AS qux2
FROM `s2`
EMIT CHANGES;
```

Your output should resemble the following. Notice the casing of the
headers that ksqlDB prints:

```
+---------------+---------------+---------------+---------------+---------------+---------------+---------------+
|foo            |BAR            |Baz            |Corge          |garply         |QUX            |QUX2           |
+---------------+---------------+---------------+---------------+---------------+---------------+---------------+
|k1             |1              |x              |v1             |5              |2              |2              |
|k2             |3              |y              |v2             |6              |4              |4              |
|k3             |5              |z              |v3             |8              |6              |6              |
```


## User-defined functions

Another area where casing is important is user-defined functions
(UDFs) that work with struct parameters. This is the case whenever you
use a UDF that either receives an incoming struct or returns a custom
struct. Any references to struct fields must exactly match the casing
ksqlDB expects. ksqlDB will reject any UDF invocations that do not
match it.

Here is a quick example of using backticks in a UDF that returns a
custom struct. The first two fields override the default behavior. The
last uses the default casing. That means that when you work with the
data returned from the UDF in ksqlDB, all select statements must use
this exact casing (see the example above). For more information on
working with structs in UDFs, see the struct section of the [how to
create a user-defined function
guide](create-a-user-defined-function.md#using-structs-and-decimals).

```java
public static final Schema AGGREGATE_SCHEMA = SchemaBuilder.struct().optional()
    .field("`min`", Schema.OPTIONAL_INT64_SCHEMA)
    .field("`max`", Schema.OPTIONAL_INT64_SCHEMA)
    .field("COUNT", Schema.OPTIONAL_INT64_SCHEMA)
    .build();
```
