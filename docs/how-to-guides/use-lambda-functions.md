# Use lambda functions

## Context

There doesn't exist any built-in UDF that suits your needs and you're unable to implement and deploy a UDF.
To get around this limitation, you use lambda functions.

## Syntax

The arguments for the lambda function are separated from the body of the lambda with a `=>`

When there are 2 or more arguments, the arguments must be enclosed with parentheses. Parentheses are optional for lambda functions with 1 argument.

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


## In action

TODO: Come up with examples and paste in




