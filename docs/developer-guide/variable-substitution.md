---
layout: page
title: ksqlDB Variable Substitution
tagline: Use variables in SQL statements
description: Learn how to use variables in SQL statements
---

Define variables
----------------

Syntax:
```
DEFINE <name> = '<value>';

Where: 
  <name>     is the variable name
  <value>    is the variable value
```

Valid variable names start with a letter or underscore (\_) followed by zero or more alphanumeric characters or underscores (_).

There is no type declaration for a value.  All variable values must be enclosed into single-quotes.

The following DEFINE statements show example assignments to variable values.
```
DEFINE replicas = '3';
DEFINE format = 'JSON';
DEFINE name = 'Tom Sawyer';
```

Single-quotes are removed during variable substitution. To escape single-quotes, enclose the value with triple-quotes.

The following DEFINE statement shows an example variable assignment that embeds single quotes in the value.
```
# becomes 'my_topic'
DEFINE topicName = '''my_topic''';      
```

Delete variables
----------------

Syntax:
```
UNDEFINE name;
```

Delete defined variables by using the `UNDEFINE` statement.

Print variables
---------------

Syntax:
```
SHOW VARIABLES;
```

The following example shows how to assign and print variable values.
```
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

Reference substitution variables
--------------------------------

Reference a variable by enclosing the variable name between `${}` characters, for example, `${replicas}`.

The following example shows how to assign and reference variable values.
```
DEFINE format = 'AVRO';
DEFINE replicas = '3';
CREATE STREAM stream1 (id INT) WITH (kafka_topic='stream1', value_format='${format}', replicas=${replicas});
```

ksqlDB doesn't add single-quotes to values during variable substitution. Also, you must know the data type of a variable to use it.

!!! note
    Variables are case-insensitive. A reference to `${replicas}` is the same as `${REPLICAS}`.

Escape substitution variables
-----------------------------

If you want to escape a variable reference, so it is not substituted, then use double `$$` characters.
```
DEFINE format = 'AVRO';
SELECT '$${format}' FROM stream;
```
The above query will become `SELECT '${format}' FROM stream`

Context for substitution variables
----------------------------------

Variable substitution is allowed in specific SQL statements. You can replace text and non-text literals, and identifiers like
column names and stream/table names. You can't use variables as reserved keywords.

The following statements show examples of using variables for stream and column names, and in other places.  

```
CREATE STREAM ${streamName} (${colName1} INT, ${colName2} STRING) \
WITH (kafka_topic='${topicName}', format='${format}', replicas=${replicas}, ...);
      
INSERT INTO ${streamName} (${colName1}, ${colName2}) \
VALUES (${val1}, '${val2}');

SELECT * FROM ${streamName} \
WHERE ${colName1} == ${val1} and ${colName2} == '${val2}' EMIT CHANGES; 
```

Using a variable in a statement that doesn't support variables causes a SQL parsing error.

Enable or disable substitution variables
----------------------------------------

Enable or disable variable substitution by setting the `ksql.variable.substitution.enable` config. You can set this config in
the server-side configuration file (`ksql-server.properties`), or you can set it in CLI and HTTP requests.

```
ksql> set 'ksql.variable.substitution.enable' = 'false';
ksql> CREATE STREAM ... WITH (kafka_topic='${topic_${env}}');
Error: Fail because ${topic_${env}} topic name is invalid. 
```
