---
layout: page
title: String Data Type
tagline: String data types
description: Syntax Reference for string data type in ksqlDB
keywords: ksqldb, sql, syntax, string, varchar, data type, character
---

| name                | description            | backing Java type
|---------------------|------------------------|------------------
| `varchar`, `string` | variable-length string | [`java.lang.String`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/String.html)

The `varchar` type represents a string in UTF-16 format.

Comparisons between `varchar` instances don't account for locale.

