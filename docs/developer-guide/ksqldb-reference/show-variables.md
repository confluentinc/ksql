---
layout: page
title: SHOW VARIABLES 
tagline: ksqlDB SHOW VARIABLES statement
description: Syntax for the SHOW VARIABLES statement in ksqlDB
keywords: variable, substitution, define
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/show-variables.html';
</script>

# SHOW VARIABLES

## Synopsis

```sql
SHOW VARIABLES;
```

## Description

Shows all currently defined variables.

## Example

```sql
ksql> DEFINE replicas = '3';
ksql> DEFINE format = 'AVRO';
ksql> DEFINE topicName = '''my_topic''';
ksql> SHOW VARIABLES;

 Variable Name | Value      
----------------------------
 replicas      | 3
 format        | AVRO         
 topicName     | 'my_topic' 
----------------------------
```
