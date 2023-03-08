# KLIP 38 - Variable Substitution

**Author**: Sergio Peña (@spena) |
**Release Target**: 0.14.0; 6.1.0 |
**Status**: _Merged_ |
**Discussion**: https://github.com/confluentinc/ksql/pull/6259

**tl;dr:** _Allow users to use variable substitution in SQL statements. Variable substitution enable users to write SQL scripts with customized output based on their environment needs._

## Motivation and background

The current plan to provide tooling and syntax for ksqlDB migrations enable users to write SQL scripts
that can be run in CI/CD environments to verify their schema will work as expected. Also, it is very common for users
to run their SQL scripts in different environments (QA, Devel, Prod, ...) to validate ksqlDB functionality. In order
to give more flexibility on customizing their SQL output, we can support variable substitution to help separate
environment-specific configuration variables from SQL code.

This KLIP proposes a new syntax to define and interact with user variables, and implementation details to allow
variable substitution in SQL commands.

Issue: https://github.com/confluentinc/ksql/issues/6199

## What is in scope

* Define a new syntax to declare and interact with user variables
* Allow variable substitution in SQL commands
* Discuss the context and scope for variable substitution
* Provide a configuration to enable/disable variable substitution
* Support variable substitution in the server-side

## What is not in scope

* `Prompt users to type values`

  Oracle SQL*Plus prompts users to type the variable if not defined. Useful, but not necessary, and it will be a limited
  support only (i.e. CLI and UI, but not Java API or other clients).

* `Provide list of pre-defined variables`

   Some DBs provide a list of pre-defined variables users can use out-of-box. We don't have any variables we'd like to add at this time.

* `Support UDF when defining variables`

  It would be nice to use functions, like `set var = now();`, `set var = uppercase('a');`, etc. But this requires clients (CLI, UI, Java API, ...) to have
  better parsing mechanism to detect UDF, make a request to the server, then assign the value to the variable. Also, there
  are functions that might be executed in the CLI context, such as NOW() which should return the current time of the CLI.

  We could define a set of functions that can be used instead, and just execute them in the local context; but this will require
  every client supports that, which will make things more complicated (i.e. the ksqlDB UI).

## Value/Return

Users will write richer SQL scripts with customized output based on user variables.

These SQL scripts may run using specific settings (topic names/format, replicas/partitions, expressions)
allowing users to easily test the behavior in different environments (CI/CD, QA clusters, Devel/Prod environments, etc).

## Public APIS

* New syntax will be added to interact with user variables
* A new CLI command and/or configuration will be added to disable/enable variable substitution
* SQL syntax will be modified to support variable references

## Design

### 1. Syntax

#### Background

ksqlDB has a syntax to SET server and CLI properties. The use is `SET` for server properties, and `SET CLI` for CLI settings.

For instance:
```
ksql> SET 'auto.offset.reset' = 'earliest';
ksql> SET CLI WRAP OFF;
```

The quickest assumption is that we can use `SET` for user variables as well. But, we cannot re-use any of the above syntax because
of conflicts with current server and CLI setting names. So a new keyword must be use.

Some ideas:
```
SET LOCAL   var1 = 'val1';
SET SESSION var1 = 'val1';
SET VAR     var1 = 'val1';
```

`LOCAL` is confusing for users coming from other DBs environments. `LOCAL` is used to keep the scope on per-transaction basis. After a statement
is executed, the variable is destroyed.

`SESSION` may be also confusing. `SESSION` is used on other DBs to override system variables for that session only. System variables are like ksqlDB
server properties.

`VAR` seems a better name.

However, we're now overloading the use of `SET` statements. For instance, in the future, we might want to use `SET ROLE`  to set the current role
fo the user when interacting with an RBAC server. But, if we want to print variable information, how would we do it?
```
SET VAR var1;  # prints var1 name
SET VAR;       # lists all variables
SET;           # what to do? lists all server, cli, variables, etc?
```

Also, the use of single-quotes in `SET` server settings is tedious to type. So after removing them, we'll have two different sets:
* `SET 'server_property' = 'server_value'`
* `SET VAR variable = 'variable_value'`

This looks confusing too. So we have to decide for a new syntax for substitution variables.

Other DBs use different syntax to create user variables (DECLARE, DEFINE, \set, SET). For ksqlDB, I'll choose to use `DEFINE` and `UNDEFINE` which
is what [Oracle SQL*plus uses](https://blogs.oracle.com/opal/sqlplus-101-substitution-variables). Any user coming from this DB will immediately understand the command.  
This is opened to discussion, but will continue with this for now.

#### Creating variables

Syntax:
```
DEFINE <name> = '<value>';

Where: 
  <name>     is the variable name
  <value>    is the variable value
```
Valid variables names start with a letter or underscore (\_) followed by zero or more alphanumeric characters or underscores.

There are no type definition for values. All variables values must be wrapped into single-quotes.

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

#### Deleting variables

Syntax:
```
UNDEFINE name;
```

You can delete defined variables with the `UNDEFINE` syntax.

#### Printing variables

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

#### Define variables during CLI execution

A new parameter will be added to the CLI command to define variables:
```
$ ksql -d id=1 -d format=JSON
or
$ ksql --define id=1 --define format=JSON
```

These parameters with the combination of `-e` or `--file` will allow users to dynamically replace
variables found in a script file (`--file`) or query string (`-e`).

Example:
```
$ ksql -d id=1 -e 'select * from table where id = ${id}'
or
$ ksql -d id=1 --file test.sql
```

### 2. Scope

Variable are temporary available during different scopes. They can be local during a HTTP request, or a session
during the opening/closing of the CLI or another ksqlDB client plugin.

Only the user that opened the CLI or HTTP request can see the defined variables. No other users can interact
with other users variables.

Context | Scope
--- | ---
HTTP requests | LOCAL
CLI | SESSION
Java API | SESSION
Headless | SESSION
RUN SCRIPT | LOCAL

#### HTTP requests

Scope: LOCAL

Variables may be defined and substituted in the server-side. The scope for the variable is during the execution of a
HTTP request.

Example:
```
# This will successfully execute the below query
Request 1:
{
  "statement" : "
    DEFINE id = '5';
    SELECT name FROM table WHERE id = ${id};   
  "
}

# This will fail because ${id} is not defined
Request 2:
{
  "statement" : "    
    SELECT name FROM table WHERE id = ${id};   
  "
}
```

NOTE: This could change in the future, once we introduced session connections with the server. But for now, the scope is kept
local to the request. The `SET` command behaves similar too.

#### CLI

Scope: SESSION

In CLI, users begin interacting with the server when starting the CLI. In this situation, the session is opened when starting the CLI with the `ksql` command, and destroyed when typing `exit`.

Currently, there is no concept of session between client/server connections. To keep true to a CLI session, the CLI will keep
defined variables in memory, and then do variable substitution prior to send the HTTP request or include each DEFINE syntax in the
request (this will be considered during implementation).

Example:
```
ksql> DEFINE id = '5';
ksql> SELECT name FROM table WHERE id = ${id};
+---------+
|  name   |
+---------+
|  Max    |
^C
Query terminated
```

#### Java API

Scope: SESSION

In Java API client, the session is opened when `Client.create()` is called, and closed when `Client.close()` is called. Variables
may be defined using a client method instead of a new syntax. This is to ensure users know the scope starts after the `create()` call.

Users will define variables using the functions, `define(variable_name, value)` and `undefine(variable_name)`.
The client will keep track of the calls made in a map. Alternatively, the `executeStatement`, `executeQuery`, `streamQuery`
`insertInto`, `streamInserts` and `createConnector` functions could accept a third parameter, `sessionVariables`
that is a map of variables to variable definitions.

There are two options for executing the variable substitution. The first is to do it in the client by
using the variable substitutor in `ksqldb-parser`. The disadvantages to this are that `ksqldb-parser`
would be a dependency of the Java client, and that the client and server versions would have
to match in order for the parser to work.

The other option is to update the endpoints `/ksql`, `/inserts-stream` and `/query-stream` to accept `sessionVariables`
as a parameter. The variable substitution would then be done on the server side. The disadvantage is
that an API change would be required.

Because the Java client is meant to be lightweight and flexible, the disadvantages of the first option
are stronger than the disadvantage of the second, so we will go with the second option.

Note: Users can use variables and replace variables in strings using Java language features too.

#### Headless Mode

In headless, the session is opened when starting the ksqlDB server with a SQL script. The variables stay alive until the server is
killed.

Variables are not persisted anywhere. They are living in memory as long as the session is not terminated.

#### RUN SCRIPT command

Scope: LOCAL

If a `RUN SCRIPT` command is executed in a CLI session, then any variable defined there will be local to the execution context of the
script. All statements from the script are sent as one single HTTP request. Variable definition and substitution happen in the server-side
without using any variables defined in the CLI session.

Note: Variables in the script file will not override session variables either. This keeps users safe to execute scripts without causing
any disruption to the variables already defined.

### 3. Referencing substitution variables

Variables will be referenced by wrapping the variable between `${}` characters (i.e. `${replicas}`).

Note: We need to make changes in QTT tests to use `${}` references instead of `{}` for consistency.

Example:
```
ksql> DEFINE format = 'AVRO';
ksql> DEFINE replicas = '3';
ksql> CREATE STREAM stream1 (id INT) WITH (kafka_topic='stream1', value_format='${format}', replicas=${replicas});
```

Substitution will not attempt to add single-quotes to the values. Users will need to know the data type to use when using a variable. The ksqlDB
server will be in charge of parsing and failing if the type is not allowed.

Note: Variables are case-insensitive. A reference to `${replicas}` is the same as `${REPLICAS}`.

### 4. Context for substitution variables

Variable substitution will be allowed in specific SQL statements. They can be used to replace text and non-text literals, and identifiers such as
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

Variables can also be assigned to other variables:
```
ksql> DEFINE var1 = 'topic';
ksql> DEFINE var2 = 'other_${var1}';
ksql> SHOW VARIABLES;

 Variable Name | Value      
----------------------------       
 var2          | other_topic 
----------------------------
```

In CLI, variables can also be assigned to set server or CLI settings:
```
ksql> DEFINE offset = 'earliest'
ksql> SET 'auto.offset.reset' = '${offset}';

ksql> DEFINE wrap_toggle = 'ON'
ksql> SET CLI WRAP ${WRAP_TOGGLE}
```

### 5. Enable/disable substitution variables

The `ksql.variable.substitution.enable` config will be used to enable/disable this feature. The config can be enabled from
the server-side configuration (ksql-server.properties), or it can be overriden by the users in the CLI or HTTP requests.

```
ksql> set 'ksql.variable.substitution.enable' = 'false';
ksql> CREATE STREAM ... WITH (kafka_topic='${topic_${env}}');
Error: Fail because ${topic_${env}} topic name is invalid. 
```
The error message is just an example of what ksqlDB will display.

### 6. Implementation

Variable substitution will happen in the server-side. This will ensure that all supported ksqlDB clients benefit from this feature.
Including the use of variables in headless mode.

The server will substitute variables that are defined in the HTTP request only.

#### CLI

To allow the CLI use all variables of the CLI session, then the CLI will also substitute variables for every command executed.
The CLI will make a request to the Server with the final statement replaced.

Example:
```
ksql> DEFINE topic1 = '''my_topic''';
ksql> CREATE STREAM s1 (...) WITH (kafka_topic=${topic1}, ...);
... the following request will be sent
{
  "statements" : "
     CREATE STREAM s1 (...) WITH (kafka_topic='my_topic', ...);
  "
}
```

#### Server

In the server side, variable substitution will be done by the `EngineContext.prepare()`, which will substitute variables from the  
parsed statement prior to call the `KsqlParser.prepare()`. This way, the `EngineContext` will get a new statement string with the
variables replaced, so it can be added to the `PreparedStatement`.

The rules for where variable substitution is allowed will be defined in `SqlBase.g4`. The parser will throw an error if a variable  
is used in an invalid place (i.e. attempting to replace SQL keywords).

SQL injection will also be avoided due to the parser walking-tree process. When a variable reference is found in the tree node, the process will
replace it with the right literal or identifier object, instead of appending the replacement as a SQL string. For instance:
```
# This way of replacement may cause SQL injection exploits if `id` expands the SQL string to include other SQL keywords.
"select * from table where id = " + id
```

DEFINE statements found in the HTTP request will be temporary available during the request execution. These variables will be
stored in a Map object and passed to the `EngineContext` to provide the variables for substitution.

## Test plan

* Unit tests to validate all new methods
* Integration tests to verify different variable substitutions (negative and positive cases)
* Include SQL injection tests that show this exploit is not possible

## LOEs and Delivery Milestones

This is a _small feature_. Changes are only done in a few places, so LOE is 1-2 weeks including testing.

## Documentation Updates

* Add new documentation for variable substitution, including examples

## Compatibility Implications

No compatibility implications.  
Variables will not be persisted in the command topic, so backwards and rollback compatibility are allowed.

## Security Implications

No security implications.
