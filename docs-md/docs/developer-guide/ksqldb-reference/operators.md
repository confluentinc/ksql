---
layout: page
title: ksqlDB Operators
tagline:  ksqlDB operators for queries
description: Operators to use in  ksqlDB statements and queries
keywords: ksqlDB, operator
---

Operators
=========

KSQL supports the following operators in value expressions.

  - [Arithmetic](#arithmetic)
  - [Concatenation](#concatenation)
  - [Source Dereference](#source-dereference)
  - [Subscript](#subscript)
  - [STRUCT dereference](#struct-dereference)

The explanation for each operator includes a supporting example based on
the following table:

```sql
CREATE TABLE USERS (
    USERID BIGINT
    FIRST_NAME STRING,
    LAST_NAME STRING,
    NICKNAMES ARRAY<STRING>,
    ADDRESS STRUCT<STREET_NAME STRING, NUMBER INTEGER>
) WITH (KAFKA_TOPIC='users', VALUE_FORMAT='AVRO', KEY='USERID');
```

Arithmetic
----------

The usual arithmetic operators (`+,-,/,*,%`) may be
applied to numeric types, like INT, BIGINT, and DOUBLE:

```sql
SELECT LEN(FIRST_NAME) + LEN(LAST_NAME) AS NAME_LENGTH FROM USERS EMIT CHANGES;
```

Concatenation
-------------

The concatenation operator  (`+,||`) can be used to
concatenate STRING values.

```sql
SELECT FIRST_NAME + LAST_NAME AS FULL_NAME FROM USERS EMIT CHANGES;
```

You can use the `+` operator for multi-part concatenation, for
example:

```sql
SELECT TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss') +
        ': :heavy_exclamation_mark: On ' +
        HOST +
        ' there were ' +
        CAST(INVALID_LOGIN_COUNT AS VARCHAR) +
        ' attempts in the last minute (threshold is >=4)'
  FROM INVALID_USERS_LOGINS_PER_HOST
  WHERE INVALID_LOGIN_COUNT>=4
  EMIT CHANGES;
```

Source Dereference
------------------

The source dereference operator (`.`) can be used
to specify columns by dereferencing the source stream or table.

```sql
SELECT USERS.FIRST_NAME FROM USERS EMIT CHANGES;
```

Subscript
---------

The subscript operator (`[subscript_expr]`) is used to
reference the value at an array index or a map key.

```sql
SELECT NICKNAMES[0] FROM USERS EMIT CHANGES;
```

STRUCT dereference
------------------

Access nested data by declaring a STRUCT and using the
dereference operator (`->`) to access its fields:

```sql
CREATE STREAM orders (
  orderId BIGINT,
  address STRUCT<street VARCHAR, zip INTEGER>) WITH (...);

SELECT address->street, address->zip FROM orders EMIT CHANGES;
```

Combine -\> with . when using aliases:

```sql
SELECT orders.address->street, o.address->zip FROM orders o EMIT CHANGES;
```