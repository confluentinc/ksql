# Use lambda functions

## Context

There doesn't exist a built-in UDF that suits your needs and you're unable to implement and deploy a UDF.
To get around this limitation, you use ksqlDB implemented lambda functions.

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

The arguments for the lambda function are separated from the body of the lambda with a `=>`.

When there are 2 or more arguments the arguments must be enclosed with parentheses. Parentheses are optional for lambda functions with 1 argument.

Currently, ksqlDB only supports up to 3 arguments in a single lambda function.

```sql
x => x + 5

(x,y) => x - y

(x,y,z) => z AND x OR y
```

## Invocation UDFs

Lambda functions can only be used inside designated invocation functions. Invocations available currently are:

- [TRANSFORM](/developer-guide/ksqldb-reference/scalar-functions#TRANSFORM)
- [REDUCE](/developer-guide/ksqldb-reference/scalar-functions#REDUCE)
- [FILTER](/developer-guide/ksqldb-reference/scalar-functions#FILTER)

## Create a Lambda Compatible Stream
Invocation functions require either a map or array input. Here, we create a stream
with a column type `MAP<STRING, INTEGER>`.
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
A lambda invocation function is a [scalar UDF](/developer-guide/ksqldb-reference/scalar-functions) and is used as such.

This lambda function transforms both the key and value of a map and produces a new map. The key is transformed
into an uppercase string using a built in UDF, and the value is transformed through addition. The order of the variables
matters, the first item in the arguments list, here `k`, is treated as the key and the second, here `v` is treated
as the value. Pay attention to this if your map has different types. Note that `transform` on
a map requires two lambda functions, while `transform` on an array requires one.
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
  3, MAP("hello":= 15, "goodbye":= -5)
);
```

Query the output.
```sql
SELECT * FROM output AS final_output;
```

You should see something similar to:
```sql
+------------------------------+------------------------------+
|id                            |final_output                  |
+------------------------------+------------------------------+
|3                             |{HELLO: 20}                   |
|4                             |{GOODBYE: 0}                  |                           
```

## Use a reduce lambda invocation function
Create a stream with a column type `ARRAY<INTEGER>` and apply the `reduce` lambda 
invocation function.
```sql
CREATE STREAM stream1 (
  id INT,
  lambda_arr ARRAY<INTEGER>
) WITH (
  kafka_topic = 'stream1',
  partitions = 1,
  value_format = 'avro'
);

CREATE STREAM output AS
  SELECT id, 
  REDUCE(lambda_arr, 2, (s, x) => ceil(x/s)) 
  FROM stream1
  EMIT CHANGES;
```
Insert some values into `stream1`.
```sql
INSERT INTO stream1 (
  id, lambda_arr
) VALUES (
  1, ARRAY(2, 3, 4, 5)
);
```

Query the output.
```sql
SELECT * FROM output AS final_output;
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
  FILTER(lambda_map, (k, v) => instr(k, 'name') > 0 AND v != 0) 
  FROM stream1
  EMIT CHANGES;
```
Insert some values into `stream1`.
```sql
INSERT INTO stream1 (
  id, lambda_arr
) VALUES (
  1, MAP("first name":= 15, "middle":= 25, "last name":= 0, "alt name":= 33)
);
```

Query the output.
```sql
SELECT * FROM output AS final_output;
```

You should see something similar to:
```sql
+------------------------------+-----------------------------------------------+
|id                            |final_output                                   |
+------------------------------+-----------------------------------------------+
|1                             |{first name: 15, alt name: 33}                 |  
```

## Advanced lambda use cases
Create a stream with a column type `MAP<STRING, ARRAY<DECIMAL(2,3)>` and apply the `transform` 
lambda invocation function with a nested `transform` lambda invocation function.
```sql
CREATE STREAM stream1 (
  id INT,
  lambda_map MAP<STRING, ARRAY<DECIMAL(2,3)>>
) WITH (
  kafka_topic = 'stream1',
  partitions = 1,
  value_format = 'avro'
);

CREATE STREAM output AS
  SELECT id, 
  TRANSFORM(lambda_map, (k, v) => concat(k, '_new')  (k, v) => transform(v, x => round(x))) 
  FROM stream1
  EMIT CHANGES;
```
Insert some values into `stream1`.
```sql
INSERT INTO stream1 (
  id, lambda_arr
) VALUES (
  1, MAP("Mary":= ARRAY[1.23, 3.65, 8.45], "Jose":= ARRAY[5.23, 1.65]})
);
```

Query the output.
```sql
SELECT * FROM output AS final_output;
```

You should see something similar to:
```sql
+------------------------------+----------------------------------------------------------+
|id                            |final_output                                              |
+------------------------------+----------------------------------------------------------+
|1                             |{Mary_new: [1, 4, 8], Jose_new: [5, 2]}                   |  
```
