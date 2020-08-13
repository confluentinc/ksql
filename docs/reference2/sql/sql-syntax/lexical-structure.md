---
layout: page
title: Lexical structure data
tagline: Structure of SQL commands and statements in ksqlDB 
description: Details about SQL commands and statements in ksqlDB 
keywords: ksqldb, sql, keyword, identifier, constant, operator
---

SQL is a domain-specific language for managing and manipulating data. It’s primarily used to work with structured data, where the types and relationships across entities are well-defined. Originally adopted for relational databases, SQL is rapidly becoming the language of choice for stream processing. It’s declarative, expressive, and ubiquitous.

The American National Standards Institute (ANSI) maintains a standard for the specification of SQL. SQL-92, the third revision to the standard, is generally the most recognized form of that specification. Beyond the standard, there are many flavors and extensions to SQL so that it can express programs beyond the SQL-92 grammar.

ksqlDB’s SQL grammar was initially built around Presto’s grammar and has been judiciously extended. ksqlDB goes beyond SQL-92 because the standard currently has no constructs for streaming queries, a core aspect of this project.

## Syntax

SQL inputs are made up of a series of commands. Each command is made up of a series of tokens and ends in a semicolon (`;`). The tokens that apply depend on the command being invoked.

A token is any keyword, identifier, backticked identifier, literal, or special character. Tokens are conventionally separated by whitespace unless there is no ambiguity in the grammar. This often happens when tokens flank a special character.

As an example, the following is syntactically valid ksqlDB SQL input:

```sql
INSERT INTO s1 (a, b) VALUES ('k1', 'v1');

CREATE STREAM s2 AS
    SELECT a, b
    FROM s1
    EMIT CHANGES;

SELECT * FROM t1 WHERE k1='foo' EMIT CHANGES;
```

## Keywords

Some tokens, such as `SELECT`, `INSERT`, and `CREATE`, are known as keywords. Keywords are reserved tokens that have a specific meaning in ksqlDB’s syntax. They control their surrounding allowable tokens and execution semantics. Keywords are case insensitive, meaning `SELECT` and `select` are equivalent. You cannot create an identifier that is already a keyword (unless you use backticked identifiers).

A complete list of keywords can be found in the appendix.

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

A complete list of operators can be found in the appendix.


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
[numeric types](../data-types/numeric.md), like INT, BIGINT, DOUBLE, and
DECIMAL.

The following example statement uses the addition operator (`+`) to compute
the sum of the lengths of two strings:

```sql
SELECT USERID, LEN(FIRST_NAME) + LEN(LAST_NAME) AS NAME_LENGTH FROM USERS EMIT CHANGES;
```

### Concatenation

Use the concatenation operator (`+` or `||`) to concatenate
[STRING](../data-types/character.md) values.

```sql
SELECT USERID, FIRST_NAME + LAST_NAME AS FULL_NAME FROM USERS EMIT CHANGES;
```

You can use the concatenation operator for multi-part concatenation.

The following example statement uses the concatenation operator to create
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

Providing the fully qualified column name is optional, unless the column name is
ambiguous. For example, if two sides of a join both contain a `foo` column, any
reference to `foo` in the query must be fully qualified.

The following example statement selects the values of the USERID and FIRST_NAME
columns in the USERS table by using the fully qualified column names:

```sql
SELECT USERS.USERID, USERS.FIRST_NAME FROM USERS EMIT CHANGES;
```

### Subscript

Use the subscript operator (`[subscript_expr]`) to reference the value at an
[array](data-types/compound.md#array) index or a [array](data-types/compound.md#map)
map key. Arrays indexes are one-based.

The following example statement selects the first string in the NICKNAMES array:

```sql
SELECT USERID, NICKNAMES[1] FROM USERS EMIT CHANGES;
```

### Struct dereference

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

There are bracketed comments in the grammar, so I think so.
'/*' .*? '*/' 

## Operator precedence

Precedence in order:

1. `*` (multiplication), `/` (division), `%` (modulus)
2. `+` (positive), - (negative), + (addition), + (concatenation), - (subtraction)
3. `=`, `>`, `<`, `>=`, `<=`, `<>`, `!=` (comparison operators)
4. NOT
5. AND
6. BETWEEN, LIKE, OR

In an expression, when two operators have the same precedence level, they're
evaluated left-to-right based on their position in the expression.

You can enclose an expression in parentheses to force precedence or clarify
precedence, for example, _(5 + 2) * 3_.