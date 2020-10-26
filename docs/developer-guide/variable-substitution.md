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
ksql> DEFINE format = 'AVRO';
ksql> DEFINE replicas = '3';
ksql> CREATE STREAM stream1 (id INT) WITH (kafka_topic='stream1', value_format='${format}', replicas=${replicas});
```

ksqlDB doesn't add single-quotes to values during variable substitution. Also, you must know the data type of a variable to use it.

!!! note
    Variables are case-insensitive. A reference to `${replicas}` is the same as `${REPLICAS}`.

Context for substitution variables
----------------------------------

Variable substitution are allowed in specific SQL statements. They can be used to replace text and non-text literals, and identifiers such as
column names and stream/table names. Variables cannot be used as reserved keywords.

For instance:

```
ksql> CREATE STREAM ${streamName} (${colName1} INT, ${colName2} STRING) \
      WITH (kafka_topic='${topicName}', format='${format}', replicas=${replicas}, ...);
      
ksql> INSERT INTO ${streamName} (${colName1}, ${colName2}) \
      VALUES (${val1}, '${val2}');

ksql> SELECT * FROM ${streamName} \
      WHERE ${colName1} == ${val1} and ${colName2} == '${val2}' EMIT CHANGES; 
```

Any attempt of using variables on non-permitted places will fail with the current SQL parsing error found when parsing the variable string.

Enable/disable substitution variables
-------------------------------------

The `ksql.variable.substitution.enable` config will be used to enable/disable this feature. The config can be enabled from
the server-side configuration (ksql-server.properties), or it can be overriden by the users in the CLI or HTTP requests.

```
ksql> set 'ksql.variable.substitution.enable' = 'false';
ksql> CREATE STREAM ... WITH (kafka_topic='${topic_${env}}');
Error: Fail because ${topic_${env}} topic name is invalid. 
```
