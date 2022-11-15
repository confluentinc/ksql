# SHOW VARIABLES

## Synopsis

```sql
DEFINE VARIABLES;
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
