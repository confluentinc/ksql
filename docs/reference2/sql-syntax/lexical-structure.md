---
layout: page
title: Lexical structure data
tagline: Structure of SQL commands and statements in ksqlDB 
description: Details about SQL commands and statements in ksqlDB 
keywords: ksqldb, sql, keyword, identifier, constant, operator
---

- ksqlDB SQL is SQL extended for streaming
- commands
  - tokens
  - terminate with ;
  - valid tokens
    - keyword
    - identifier
    - backticked identifier
    - special character
  - tokens separated by whitespace

[ example commands ]

- Note that it's based off Presto + has additions for other functionality.

## Keywords

https://github.com/confluentinc/ksql/blob/master/ksqldb-parser/src/main/antlr4/io/confluent/ksql/parser/SqlBase.g4

- Keywords = reserved words
  - examples: select, insert, where
  - case insensitive

## Identifiers

- Identifiers = user-space symbols
- Identify streams, tables, and other objects created by the user
  - examples: I create a stream called `s1`. `s1` is an identifier
- case insensitive

[ example ]

TODO: what makes a valid identifier?

- Backticked identifiers = escaped for exact casing
- Allows you to use any name of your choosing, including keywords.
- This is useful if you don't control the data

[ example ]

## Constants

- A value of a given type - a literal
- String constants - single quotes
- Numeric constants
- Boolean constants

## Operators

The following table lists all of the operators that are supported by ksqlDB SQL.

| operator     | meaning                        | applies to      
|--------------|--------------------------------|-----------------
| `=`          | is equal to                    | string, numeric 
| `!=` or `<>` | is not equal to                | string, numeric 
| `<`          | is less than                   | numeric         
| `<=`         | is less than or equal to       | numeric         
| `>`          | is greater than                | numeric         
| `>=`         | is greater than or equal to    | numeric         
| `+`          | addition and concatenation     | numeric         
| `-`          | subtraction                    | numeric         
| `*`          | multiplication                 | numeric         
| `/`          | division                       | numeric         
| `%`          | wildcard and modulus           | string, numeric 
| `||` or `+`  | concatenation                  | string          
| `:=`         | assignment                     | all             
| `->`         | struct field dereference       | struct     
| `.`          | source dereference             | table, stream
| `E`          | exponent                       | numeric         

The explanation for each operator includes a supporting example based on
the following table:

```sql
CREATE TABLE USERS (
    USERID BIGINT PRIMARY KEY,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    NICKNAMES ARRAY<STRING>,
    ADDRESS STRUCT<STREET_NAME STRING, HOUSE_NUM INTEGER>
) WITH (KAFKA_TOPIC='users', VALUE_FORMAT='AVRO');
```

### Arithmetic

You can apply the familiar arithmetic operators, like `+` and `%`, to
[numeric types](data-types/numeric.md), like INT, BIGINT, and DOUBLE.

The following example statement uses the addition operator (`+`) to compute
the sum of the lengths of two strings:

```sql
SELECT USERID, LEN(FIRST_NAME) + LEN(LAST_NAME) AS NAME_LENGTH FROM USERS EMIT CHANGES;
```

### Concatenation

Use the concatenation operator (`+` or `||`) to concatenate
[STRING](data-types/character.md) values.

```sql
SELECT USERID, FIRST_NAME + LAST_NAME AS FULL_NAME FROM USERS EMIT CHANGES;
```

You can use the `+` operator for multi-part concatenation.

The following example statement uses the concatenation operator (`+`) to create
an error message:

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

### Source dereference

Use the source dereference operator (`.`) to specify columns by dereferencing
the source stream or table.

The following example statement selects the value of the FIRST_NAME column in
the USERS table:

```sql
SELECT USERID, USERS.FIRST_NAME FROM USERS EMIT CHANGES;
```

### Subscript

Use the subscript operator (`[subscript_expr]`) to reference the value at an
[array](data-types/compund.md#array) index or a [array](data-types/compund.md#array) map key. Arrays indexes are one-based.

The following example statement selects the first string in the NICKNAMES array:

```sql
SELECT USERID, NICKNAMES[1] FROM USERS EMIT CHANGES;
```

### STRUCT dereference

Access nested data by declaring a STRUCT and using the dereference operator
(`->`) to access its fields.

The following example statement selects the values of the STREET and HOUSE_NUM
fields in the ADDRESS struct: 

```sql
SELECT USERID, ADDRESS->STREET, ADDRESS->HOUSE_NUM FROM USERS EMIT CHANGES;
```

Combine `->` with `.` when using aliases:

```sql
SELECT USERID, USERS.ADDRESS->STREET, U.ADDRESS->STREET FROM USERS U EMIT CHANGES;
```



## Special characters

Not a function/operator/identifier, have meta-meaning for the commands.

- Parens
- brackets
- commas
- semi-colon
- colon
- asterisk
- dot

## Comments

- -- double dash

TODO: does ksqlDB have multi-line comments?

bracketed comments
'/*' .*? '*/' 

## Operator precedence

TODO: can we lift this out of our grammar