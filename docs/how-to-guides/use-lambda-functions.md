---
layout: page
title: How to transform columns with structured data.
tagline: Transform columns of structured data without user-defined functions.
description: ksqlDB can compose existing functions to create new expressions over structured data
keywords: function, lambda, aggregation, user-defined function, ksqlDB  
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/how-to-guides/use-lambda-functions.html';
</script>

# Use lambda functions

## Context

You want to transform a column with structured data in a particular way, but there doesn't 
exist a built-in function that suits your needs and you're unable to implement and deploy a 
user-defined function. ksqlDB is capable of composing existing functions to create 
new expressions over structured data. These are called lambda functions.

## In action
```sql
CREATE STREAM stream1 (
  id INT,
  lambda_map MAP<STRING, INTEGER>
) WITH (
  kafka_topic = 'stream1',
  partitions = 1,
  value_format = 'avro'
);

CREATE STREAM output AS
  SELECT id, 
  TRANSFORM(lambda_map, (k, v) => UCASE(k), (k, v) => v + 5) 
  FROM stream1
  EMIT CHANGES;
```

## Syntax

The arguments for the lambda function are separated from the body of the lambda with the lambda operator, `=>`.

When there are two or more arguments, you must enclose the arguments with parentheses. Parentheses are optional for lambda functions with one argument.

Currently, ksqlDB supports up to three arguments in a single lambda function.

```sql
x => x + 5

(x,y) => x - y

(x,y,z) => z AND x OR y
```

## Invocation UDFs

Lambda functions must be used inside designated invocation functions. These are the available Invocations:

- [TRANSFORM](/developer-guide/ksqldb-reference/scalar-functions#TRANSFORM)
- [REDUCE](/developer-guide/ksqldb-reference/scalar-functions#REDUCE)
- [FILTER](/developer-guide/ksqldb-reference/scalar-functions#FILTER)

## Create a lambda-compatible stream
Invocation functions require either a map or array input. The following example creates a stream
with a column type of `MAP<STRING, INTEGER>`.
```sql
CREATE STREAM stream1 (
  id INT,
  lambda_map MAP<STRING, INTEGER>
) WITH (
  kafka_topic = 'stream1',
  partitions = 1,
  value_format = 'avro'
);
```

## Apply a lambda invocation function
A lambda invocation function is a [scalar UDF](/developer-guide/ksqldb-reference/scalar-functions), and you use it like other scalar functions.

The following example lambda function transforms both the key and value of a map and produces a new map. A built-in UDF transforms the key 
into an uppercase string using a built-in UDF, and the value is transformed through addition. The order of the variables 
is important: the first item in the arguments list, named `k` in this example, is treated as the key, and the second, 
named `v` in this example, is treated as the value. Pay attention to this if your map has different types. 
Note that `transform` on a map requires two lambda functions, while `transform` on an array requires one.
```sql
CREATE STREAM output AS
  SELECT id, 
  TRANSFORM(lambda_map, (k, v) => UCASE(k), (k, v) => v + 5) 
  FROM stream1;
```

Insert some values into `stream1`.
```sql
INSERT INTO stream1 (
  id, lambda_map
) VALUES (
  3, MAP('hello':= 15, 'goodbye':= -5)
);
```

Query the output. Make sure to set `auto.offset.reset = earliest`.
```sql
SELECT * FROM output AS final_output EMIT CHANGES;
```

Your output should resemble:
```sql
+------------------------------+------------------------------+
|id                            |final_output                  |
+------------------------------+------------------------------+
|3                             |{HELLO: 20}                   |
|4                             |{GOODBYE: 0}                  |                           
```

## Use a reduce lambda invocation function
The following example creates a stream with a column type `ARRAY<INTEGER>` and applies the `reduce` lambda 
invocation function.
```sql
CREATE STREAM stream2 (
  id INT,
  lambda_arr ARRAY<INTEGER>
) WITH (
  kafka_topic = 'stream2',
  partitions = 1,
  value_format = 'avro'
);

CREATE STREAM output2 AS
  SELECT id, 
  REDUCE(lambda_arr, 2, (s, x) => ceil(x/s)) 
  FROM stream2
  EMIT CHANGES;
```
Insert some values into `stream2`.
```sql
INSERT INTO stream2 (
  id, lambda_arr
) VALUES (
  1, ARRAY[2, 3, 4, 5]
);
```

Query the output. Make sure to set `auto.offset.reset = earliest`.
```sql
SELECT * FROM output2 AS final_output EMIT CHANGES;
```

You should see something similar to:
```sql
+------------------------------+------------------------------+
|id                            |final_output                  |
+------------------------------+------------------------------+
|1                             |{output:3}                    |  
```

## Use a filter lambda invocation function
Create a stream with a column type `MAP<STRING, INTEGER>`and apply the `filter` lambda 
invocation function. 
```sql
CREATE STREAM stream3 (
  id INT,
  lambda_map MAP<STRING, INTEGER>
) WITH (
  kafka_topic = 'stream3',
  partitions = 1,
  value_format = 'avro'
);

CREATE STREAM output3 AS
  SELECT id, 
  FILTER(lambda_map, (k, v) => instr(k, 'name') > 0 AND v != 0) 
  FROM stream3
  EMIT CHANGES;
```
Insert some values into `stream3`.
```sql
INSERT INTO stream3 (
  id, lambda_map
) VALUES (
  1, MAP('first name':= 15, 'middle':= 25, 'last name':= 0, 'alt name':= 33)
);
```

Query the output. Make sure to set `auto.offset.reset = earliest`.
```sql
SELECT * FROM output3 AS final_output EMIT CHANGES;
```

Your output should resemble:
```sql
+------------------------------+-----------------------------------------------+
|id                            |final_output                                   |
+------------------------------+-----------------------------------------------+
|1                             |{first name: 15, alt name: 33}                 |  
```

## Advanced lambda use cases
The following example creates a stream with a column type `MAP<STRING, ARRAY<DECIMAL(2,3)>` and applies the `transform` 
lambda invocation function with a nested `transform` lambda invocation function.
```sql
CREATE STREAM stream4 (
  id INT,
  lambda_map MAP<STRING, ARRAY<DECIMAL(2,3)>>
) WITH (
  kafka_topic = 'stream4',
  partitions = 1,
  value_format = 'avro'
);

CREATE STREAM output4 AS
  SELECT id, 
  TRANSFORM(lambda_map, (k, v) => concat(k, '_new')  (k, v) => transform(v, x => round(x))) 
  FROM stream4
  EMIT CHANGES;
```
Insert some values into `stream4`.
```sql
INSERT INTO stream4 (
  id, lambda_map
) VALUES (
  1, MAP('Mary':= ARRAY[1.23, 3.65, 8.45], 'Jose':= ARRAY[5.23, 1.65]})
);
```

Query the output. Make sure to set `auto.offset.reset = earliest`.
```sql
SELECT * FROM output4 AS final_output EMIT CHANGES;
```

Your output should resemble:
```sql
+------------------------------+----------------------------------------------------------+
|id                            |final_output                                              |
+------------------------------+----------------------------------------------------------+
|1                             |{Mary_new: [1, 4, 8], Jose_new: [5, 2]}                   |  
```
