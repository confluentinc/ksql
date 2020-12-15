# DEFINE

## Synopsis

```sql
DEFINE <name> = '<value>';
```

Where:

```
<name>  is the variable name
<value> is the variable value
```

## Description

Defines a variable to be used within SQL statements. Reference the variable with `${variable}` syntax.

Valid variable names start with a letter or underscore and are followed by zero or more alphanumeric characters or underscores.

All variable values must be enclosed into single-quotes. Single-quotes are removed during variable substitution. To escape single-quotes, enclose the value with triple-quotes.

There is no type declaration for a value.

## Example

```sql
DEFINE replicas = '3';
DEFINE format = 'JSON';
DEFINE name = 'Tom Sawyer';
DEFINE topicName = '''my_topic'''; -- becomes 'my_topic'
```
