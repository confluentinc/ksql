---
layout: page
title: ASSERT SCHEMA
tagline:  ksqlDB ASSERT SCHEMA statement syntax
description: Assert the existence of a schema.
keywords: ksqlDB, assert, schema
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/assert-schema.html';
</script>

## Synopsis

```sql
ASSERT (NOT EXISTS)? SCHEMA (SUBJECT subjectName)? (ID id)? (TIMEOUT timeout); 
```

## Description

Asserts that a schema exists or does not exist.

Schemas can be specifed by either their subject name, id or both.

The `TIMEOUT` clause specifies the amount of time to wait for the assertion to succeed before failing.
If the `TIMEOUT` clause is not present, then ksqlDB will use the timeout specified by the server
configuration `ksql.assert.schema.default.timeout.ms`, which is 1000 ms by default. 

If the assertion fails, then an error will be returned. 
