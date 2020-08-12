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

- Arrays start at 1

## Key selection

- -> lifting out of a struct

## Field selection

- dot to lift out of a map

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