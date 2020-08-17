---
layout: page
title: Value Statements
tagline: Syntax for accessing values in ksqlDB
description: How to get data from data types, functions, and windows in ksqlDB 
keywords: ksqldb, sql, value, dereference, aggregate, function, operator, cast
---

## Column references

- dot to get the column of a targeted collection

## Subscripts

Use the subscript operator (`[subscript_expr]`) to reference the value at an
[array](data-types/compound.md#array) index or a [array](data-types/compound.md#map)
map key. Arrays indexes are one-based.

The following example statement selects the first string in the NICKNAMES array:

```sql
SELECT USERID, NICKNAMES[1] FROM USERS EMIT CHANGES;
```

- TODO: Arrays start at 1

## Key selection

Access nested data by declaring a STRUCT and using the dereference operator
(`->`) to access its fields.

The following example statement selects the values of the STREET and HOUSE_NUM
fields in the ADDRESS struct: 

```sql
SELECT USERID, ADDRESS->STREET, ADDRESS->HOUSE_NUM FROM USERS EMIT CHANGES;
```

Combine `->` with `.` to provide fully qualified names:

```sql
SELECT USERID, USERS.ADDRESS->STREET, U.ADDRESS->STREET FROM USERS U EMIT CHANGES;
```

## Field selection

Use the source dereference operator (`.`) to specify columns by dereferencing
the source stream or table.

Providing the fully qualified column name is optional, unless the column name is
ambiguous. For example, if two sides of a join both contain a `foo` column, any
reference to `foo` in the query must be fully qualified.

The following example statement selects the values of the USERID and FIRST_NAME
columns in the USERS table by using the fully qualified column names:

```sql
SELECT USERS.USERID, USERS.FIRST_NAME FROM USERS EMIT CHANGES;
```

## Operator invocations

grammar:
- prefix: functions
- infix: operators

## Function calls

- identifier followed by arg...*
- Example

## Aggregate expressions

- identifier followed by args
- Can select identifiers from the group by or aggregated columns
- convention to use latest_by_offset to carry through values

## Window function calls

- Aggregations over rows within a span of time
- grammar breakdown

## Type casts

- changing from one data type to another
- example
- can check type coherence at query-submission time, but can't know if the cast will be valid at runtime

## Array constructors

- Brackets
- Example

## Struct constructors

- literal
- Example

## Map constructors

- angle brackets
- Example

## Expression evaluation rules

- TODO: is there an order of evaluation to exprs?