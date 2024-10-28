---
layout: page
title: ksqlDB Variable Substitution
tagline: Use variables in SQL statements
description: Learn how to use variables in SQL statements
keywords: variable, substitution, define
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/how-to-guides/substitute-variables.html';
</script>

## Context

You have a set of SQL statements, and you want to vary the exact content depending on where you use them. To do that, ksqlDB supports *variables* so that you can supply different values.

## In action

```sql
DEFINE format = 'AVRO';
DEFINE replicas = '3';

CREATE STREAM stream1 (
  id INT
) WITH (
  kafka_topic = 'stream1',
  value_format = '${format}',
  replicas = ${replicas}
);
```

## Use a variable

Begin by defining one or more variables with a [compliant name](../developer-guide/ksqldb-reference/define.md):

```sql
DEFINE format = 'AVRO';
DEFINE replicas = '3';
```

Now reference each variable by enclosing it between `${}` characters:

```sql
CREATE STREAM stream1 (
  id INT
) WITH (
  kafka_topic = 'stream1',
  value_format = '${format}',
  replicas = ${replicas}
);
```

!!! note
    Variables are case-insensitive. A reference to `${replicas}` is the same as `${REPLICAS}`.

At any point, you can list all variables to see what is available:

```sql
SHOW VARIABLES;
```

Which should output:

```text
 Variable Name | Value      
----------------------------
 replicas      | 3
 format        | AVRO
----------------------------
```

If you want to undefine a variable, you can unbound the name like so:

```sql
UNDEFINE replicas;
```

!!! note
    Single-quotes are removed during variable substitution. To escape single-quotes, enclose the value with triple-quotes.

## Escape substitution variables

If you want to escape a variable reference so it is not substituted, use double `$$` characters.

```sql
DEFINE format = 'AVRO';
SELECT '$${format}' FROM stream;
```

The above query will become `SELECT '${format}' FROM stream`.

## Context for substitution variables

Variable substitution is allowed in specific SQL statements. You can replace text and non-text literals, and identifiers like
column names and stream/table names. You can't use variables as reserved keywords.

The following statements show examples of using variables for stream and column names, and in other places.  

```sql
CREATE STREAM ${streamName} (
  ${colName1} INT,
  ${colName2} STRING
) WITH (
  kafka_topic = '${topicName}',
  format = '${format}',
  replicas = ${replicas},
  ...
);
      
INSERT INTO ${streamName} (
  ${colName1},
  ${colName2}
) VALUES (
  ${val1},
  '${val2}'
);

SELECT * FROM ${streamName} EMIT CHANGES;
```

Using a variable in a statement that doesn't support variables causes a SQL parsing error.

## Disable substitution variables

Enable or disable variable substitution by setting the [ksql.variable.substitution.enable](../../operate-and-deploy/installation/server-config/config-reference/#ksqlvariablesubstitutionenable) server configuration parameter.

```sql
ksql> SET 'ksql.variable.substitution.enable' = 'false';
ksql> CREATE STREAM ... WITH (kafka_topic='${topic_${env}}');
Error: Fail because ${topic_${env}} topic name is invalid. 
```
