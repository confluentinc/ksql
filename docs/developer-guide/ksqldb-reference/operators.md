---
layout: page
title: ksqlDB Operators
tagline:  ksqlDB operators for queries
description: Operators to use in  ksqlDB statements and queries
keywords: ksqlDB, operator
---

Operators
=========

ksqlDB supports the following operators in value expressions.

Arithmetic
----------

The usual arithmetic operators (`+,-,/,*,%`) may be
applied to numeric types, like INT, BIGINT, and DOUBLE:

```sql
SELECT USERID, LEN(FIRST_NAME) + LEN(LAST_NAME) AS NAME_LENGTH FROM USERS EMIT CHANGES;
```

Concatenation
-------------

The concatenation operator  (`+,||`) can be used to
concatenate STRING values.

```sql
SELECT USERID, FIRST_NAME + LAST_NAME AS FULL_NAME FROM USERS EMIT CHANGES;
```

You can use the `+` operator for multi-part concatenation, for
example:

```sql
SELECT USERID,
    TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss') +
        ': :heavy_exclamation_mark: On ' +
        HOST +
        ' there were ' +
        CAST(INVALID_LOGIN_COUNT AS VARCHAR) +
        ' attempts in the last minute (threshold is >=4)'
  FROM INVALID_USERS_LOGINS_PER_HOST
  WHERE INVALID_LOGIN_COUNT>=4
  EMIT CHANGES;
```

In
--

The IN operator enables specifying multiple values in a `WHERE` clause.

It provides the equivalent of multiple `OR` conditions.

Currently, this is only supported for Pull Queries.

```sql
SELECT * FROM USERS WHERE USERID IN (1543, 6256, 87569);
```

Source Dereference
------------------

The source dereference operator (`.`) can be used
to specify columns by dereferencing the source stream or table.

```sql
SELECT USERID, USERS.FIRST_NAME FROM USERS EMIT CHANGES;
```

Subscript
---------

The subscript operator (`[subscript_expr]`) is used to
reference the value at an array index or a map key.

```sql
SELECT USERID, NICKNAMES[1] FROM USERS EMIT CHANGES;
```

STRUCT dereference
------------------

Access nested data by declaring a STRUCT and using the
dereference operator (`->`) to access its fields:

```sql
SELECT USERID, ADDRESS->STREET, ADDRESS->HOUSE_NUM FROM USERS EMIT CHANGES;
```

Combine `->` with `.` when using aliases:

```sql
SELECT USERID, USERS.ADDRESS->STREET, U.ADDRESS->STREET FROM USERS U EMIT CHANGES;
```

For more information on nested data, see [STRUCT](../syntax-reference.md#struct).

