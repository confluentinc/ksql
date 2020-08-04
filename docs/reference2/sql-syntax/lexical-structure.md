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

- Infix functions like +, -, *, /.

TODO: What are all of the operators?

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


## Operator precedence

TODO: can we lift this out of our grammar