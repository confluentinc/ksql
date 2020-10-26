---
layout: page
title: ksqlDB Variable Substitution
tagline: Use variables in SQL statements
description: Learn how to use variables in SQL statements
---------------------------------------------------------

Define variables
----------------

Syntax:
```
DEFINE <name> = '<value>';

Where: 
  <name>     is the variable name
  <value>    is the variable value
```

Valid variables names start with a letter or underscore (\_) followed by zero or more alphanumeric characters or underscores (_).

There are no type definition for values.  All variables values must be wrapped into single-quotes.

Example:
```
DEFINE replicas = '3';
DEFINE format = 'JSON';
DEFINE name = 'Tom Sawyer';
```

Single-quotes are removed during variable substitution. If you need to escape single-quotes, then wrap the value with triple-quotes.

Example:
```
# becomes 'my_topic'
DEFINE topicName = '''my_topic''';      
```

Deleting variables
------------------

Syntax:
```
UNDEFINE name;
```

You can delete defined variables with the `UNDEFINE` syntax.

Printing variables
------------------

Syntax:
```
SHOW VARIABLES;
```

Example:
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

Referencing substitution variables
----------------------------------

Variables are referenced by wrapping the variable name between `${}` characters (i.e. `${replicas}`).

Example:
```
ksql> DEFINE format = 'AVRO';
ksql> DEFINE replicas = '3';
ksql> CREATE STREAM stream1 (id INT) WITH (kafka_topic='stream1', value_format='${format}', replicas=${replicas});
```

Substitution will not attempt to add single-quotes to the values. You need to know the data type to use when using a variable.

Note: Variables are case-insensitive. A reference to `${replicas}` is the same as `${REPLICAS}`.

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
