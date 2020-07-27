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
    `a` VARCHAR KEY,
    `B` INT,
    `c` VARCHAR
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
to control the casing of the stream name and two of the column names,
`a` and `B`. `c` receives the default behavior of uppercasing.

```sql
CREATE STREAM `s2` (
    `a` VARCHAR KEY,
    `B` INT,
    c VARCHAR
) WITH (
    kafka_topic = 's2',
    partitions = 1,
    value_format = 'avro'
);
```

Insert some rows into `s2`. Notice how you need to use backticks each
time to reference a field that doesn't have the default casing:

```sql
INSERT INTO `s2` (`a`, `B`, c) VALUES ('k1', 1, 'x');
INSERT INTO `s2` (`a`, `B`, c) VALUES ('k2', 2, 'y');
INSERT INTO `s2` (`a`, `B`, c) VALUES ('k3', 3, 'z');
```

Issue a push query to select the rows. The relevant identifiers are
again surrounded with backticks:

```sql
select `a`, `B`, c from `s2` emit changes;
```

Your output should resemble the following. Notice the casing of the
headers that ksqlDB prints:

```
+----------------------------------------+----------------------------------------+----------------------------------------+
|a                                       |B                                       |C                                       |
+----------------------------------------+----------------------------------------+----------------------------------------+
|k1                                      |1                                       |x                                       |
|k2                                      |2                                       |y                                       |
|k3                                      |3                                       |z                                       |
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
this exact casing. For more information on working with structs in
UDFs, see the struct section of the [wow to create a user-defined
function guide](create-a-user-defined-function.md#using-structs-and-decimals).

```java
public static final Schema AGGREGATE_SCHEMA = SchemaBuilder.struct().optional()
    .field("`min`", Schema.OPTIONAL_INT64_SCHEMA)
    .field("`max`", Schema.OPTIONAL_INT64_SCHEMA)
    .field("COUNT", Schema.OPTIONAL_INT64_SCHEMA)
    .build();
```