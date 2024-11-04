---
layout: page
title: CREATE TYPE
tagline:  ksqlDB CREATE TYPE statement
description: Syntax for the CREATE TYPE statement in ksqlDB
keywords: ksqlDB, create, type, alias, struct
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/create-type.html';
</script>

CREATE TYPE
===========

Synopsis
--------

```sql
CREATE TYPE <type_name> AS <type>;
```

Description
-----------

Create an alias for a complex type declaration.

The CREATE TYPE statement registers a type alias directly in KSQL. Any types
registered by using this command can be leveraged in future statements. The
CREATE TYPE statement works in interactive and headless modes.

Any attempt to register the same type twice, without a corresponding DROP TYPE
statement, will fail.

Example
-------

```sql
CREATE TYPE ADDRESS AS STRUCT<number INTEGER, street VARCHAR, city VARCHAR>;
```

Use the ADDRESS custom type in a statement:

```sql
CREATE TYPE PERSON AS STRUCT<name VARCHAR, address ADDRESS>;
```

