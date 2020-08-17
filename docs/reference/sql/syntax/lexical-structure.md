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

Identifiers are symbols that represent user-space entities, like streams, tables, columns, and other objects. For example, if you have a stream named `s1`, `s1` is an _identifier_ for that stream. By default, identifiers are case-insensitive, meaning `s1` and `S1` refer to the same stream. Under the covers, ksqlDB will capitalize all of the characters in the identifier for all future display purposes.

Unless an identifier is backticked, it may only be composed of characters that are a letter, number, or underscore. There is no imposed limit on the number of characters.

To make it possible to use any character in an identifier, you can surround it in backticks (``` ` ```) when it is declared and used. A _backticked identifier_ is useful when you don't control the data, so it might have special characters, or even keywords. When you use backticked identifers, the case is captured exactly, and any future references to the identifer become case-sensitive. As an example, if you declare the following stream:

```sql
CREATE STREAM `s1` (
    k VARCHAR KEY,
    `@MY-identifier-stream-column!` INT
) WITH (
    kafka_topic = 's1',
    partitions = 3,
    value_format = 'json'
);
```

You must select from it by backticking the stream name and column name and using the original casing:

```sql
SELECT `@MY-identifier-stream-column!` FROM `s3` EMIT CHANGES;
```

## Constants

There are three implicitly typed constants, or literals, in ksqlDB: strings, numbers, and booleans.

### String constants

A string constant is an arbitrary series of characters surrounded by single quotes (`'`), like `'Hello world'`. To include a quote inside of a string literal, escape the quote by prefixing it with another quote, as in `'You can call me ''Stuart'', or Stu.'`

### Numeric constants

Numeric constants are accepted in these forms:

1. **_`digits`_**
2. **_`digits`_**`.[`**_`digits`_**`][e[+-]`**_`digits`_**`]`
3. `[`**_`digits`_**`].`**_`digits`_**`[e[+-]`**_`digits`_**`]`
4. **_`digits`_**`e[+-]`**_`digits`_**

where **_`digits`_** is one or more single-digit integers (`0` through `9`). At least one digit must be present before or after the decimal point, if there is one. At least one digit must follow the exponent symbol `e`, if there is one. Spaces and underscores (nor any other characters) are allowed in the constant.

Numeric constants may also have a `+` or `-` prefix, but this considered a function applied to the constant, not the constant itself.

Here are some examples of valid numeric constants:

- `5`
- `7.2`
- `0.0087`
- `1.`
- `.5`
- `1e-3`
- `1.332434e+2`
- `+100`
- `-250`

### Boolean constants

A boolean constant is represented as either the identifer `true` or `false`. Boolean constants are not case-sensitive, meaning `true` evaluates to the same value as `TRUE`.

## Operators

Operators are infix functions composed of special characters. A complete list of operators can be found in the appendix. ksqlDB does not allow you to add user-space operators.

## Special characters

Some characters have a particular meaning that doesn't correspond to an operator. The following list describes the special characters and their purpose:

1. Parentheses (`()`) retain their usual meaning in programming languages for grouping expressions and controlling the order of evaluation.
2. Brackets (`[]`) are used to work with arrays, both in their construction and subscript access.
3. Commas (`,`) delineate a discrete list of entities.
4. The semi-colons (`;`) terminates a SQL command.
5. The asterisk (`*`), when used in particular syntax, is used as an "all" qualifier. This is seen most commonly in a `SELECT` command to retrieve all columns.
6. The period (`.`) accesses a column in a stream or table.
7. The arrow (`->`) accesses a field in a struct data type.

## Comments

A comment is a string beginning with twos dashes. It includes all of the content from the dashes to the end of the line:

```sql
-- Here is a comment.
```

You can also span a comment over multiple lines by using C-style syntax:

```sql
/* Here is
   another comment.
 */
```

## Lexical precedence

Operators are evaluated using the following order of precedence:

1. `*`, `/`, `%`
2. `+`, `-`
3. `=`, `>`, `<`, `>=`, `<=`, `<>`, `!=`
4. `NOT`
5. `AND`
6. `BETWEEN`, `LIKE`, `OR`

In an expression, when two operators have the same precedence level, they're
evaluated left-to-right based on their position.

You can enclose an expression in parentheses to force precedence or clarify
precedence, for example, `(5 + 2) * 3`.