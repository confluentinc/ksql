# KLIP 34 - Variable Substitution

**Author**: Sergio Peña (@spena) |
**Release Target**: 0.14 |
**Status**: _In Discussion_ |
**Discussion**: TBD

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
* Support variable substitution in interactive and headless mode

## What is not in scope

* Provide support to prompt users to type a variable value if it wasn't defined (Other DBs support this)
* Support variable substitution of environment and other non-user variables
* Provide a set of pre-defined variables to use in the current session
* Support variable substitution in the REST API (cannot store variables after consecutive requests)

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

#### New syntax decision

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
* `SET VAR variable = `variable_value'

This looks confusing too. So we have to decide for a new syntax for substitution variables.

Other DBs use different syntax to create user variables (DECLARE, DEFINE, \set, SET). For ksqlDB, I'll choose to use `DEFINE` and `UNDEFINE` which
is what Oracle SQL*plus uses. Any user coming from this DB will immediately understand the command.  
This is opened to discussion, but will continue with this for now.

#### Creating and printing substitution variables

The command to interact with substitution variables is `DEFINE`.  
`DEFINE` will allow us to define a new variable, print the variable value and list all current defined variables.

Syntax:
```
DEFINE [name [= 'value']];

Where: 
  name     is the variable name to define
  value    is the variable value
```
Valid variables names start with a letter or underscore (\_) followed by zero or more alphanumeric characters or underscores.

All variable values need to be wrapped using single quotes. All variables are stored as string values in memory, and when
replaced, the value without the quotes will be used. ksqlDB parsing will be in charge of determining the format at that
time.

The reason not to remove single-quotes from values is to allow spaces, and to leave the syntax open for using data types in the future if required.  
Also, having decide the format at run-time (i.e. `define n = 3.34238`) is tricky as we could lose precision in case of decimals. Better stay with Strings for now.

Define a new variable:
```
ksql> DEFINE replicas = '3';
ksql> DEFINE partitions = '5';
```

Print a defined variable value:
```
ksql> DEFINE replicas;
DEFINE replicas   = '3'
```

List all defined variables:
```
ksql> DEFINE;
DEFINE replicas   = '3'
DEFINE partitions = '5'
```

#### Deleting substitution variables

The `UNDEFINE` command deletes the variable.

Syntax:
```
UNDEFINE name;
```

### 2. Scope

This variable has a session scope.

Variables will live during the session of a ksqlDB connection, then destroyed when the connection is closed. Only the user
in that session can interact with the defined substitution variables. No other users can interact with other users' variables.

In CLI, the session is opened when starting the CLI with the `ksql` command, and destroyed when typing `exit`.

In Java API client, the session is opened when `Client.create()` is called, and closed when `Client.close()` is called. Variables
may be defined using a client method instead of a new syntax. This is to ensure users know the scope starts after the `create()` call.

TBD: Define a Java API method for variable substitution.

In headless, the session is opened when starting the ksqlDB server with a SQL script. The variables stay alive until the server is
killed.

Variables are not persisted anywhere. They are living in memory as long as the session is not terminated.

#### RUN SCRIPT command

If a `RUN SCRIPT` command is executed in a CLI session, then any variable defined there will be local to the execution context of the
script. The variables will override any variable previously defined in the session. When the script is completed, all local variables
defined will be destroyed. This ensures users do not override their session variables by accident when running `RUN SCRIPT` commands.

#### The SOURCE command

To allow users load variables and keep their scope in the session, then the new `SOURCE` command will be added. This will override
any previously defined variables, and will be destroyed until the user leaves the session.

Syntax:
```
SOURCE '<script_file>';
```

The `SOURCE` command will work like a `RUN SCRIPT`. If other SQL statements are in the file, then those statements will also be
executed, thus to avoid limiting users from using DDL/DML commands while defining variables at the same time from one single script.

`SOURCE` will be used only on the CLI and headless mode. REST API will not accept it because files may be out of the scope of the server filesystem.

### 3. Referencing substitution variables

Variables will be referenced by wrapping the variable between `${}` characters (i.e. `${replicas}`).

Noe: We need to make changes in QTT tests to use `${}` references instead of `{}` for consistency.

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

Note: `{VAR}` is used a reference where variables are permitted.
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
ksql> DEFINE var2;
DEFINE var2    = 'other_topic'
```

Variables can also be assigned to set server or CLI settings:
```
ksql> DEFINE offset = 'earliest'
ksql> SET 'auto.offset.reset' = '${offset}';

ksql> DEFINE wrap_toggle = 'ON'
ksql> SET CLI WRAP ${WRAP_TOGGLE}
```

Variables can also be used as nested variables:
```
ksql> DEFINE env = 'prod';

ksql> DEFINE topic_prod = 'my_prod_topic';
ksql> DEFINE topic_qa = 'my_qa_topic';

ksql> CREATE STREAM ... WITH (kafka_topic='${topic_${env}}'); 
```

### 5. Enable/disable substitution variables

When using CLI, we'll use the `SET CLI VARIABLE-SUBSTITUTION (ON|OFF)` command to enable/disable subsitution. Default will be `ON`.
```
ksql> SET CLI VARIABLE-SUBSTITUTION OFF;
ksql> CREATE STREAM ... WITH (kafka_topic='${topic_${env}}');
Error: Fail because ${topic_${env}} topic name is invalid. 
```
The error message is just an example of what ksqlDB will display.

When using SQL scripts in headless mode, we need to provide a server setting to enable/disable variable substitution. The setting will be
`ksql.enable.headless.variable.substitution` as Boolean (default = true). The reason is that `SET CLI` is a CLI-specific command not available
in headless execution. Also, CLI should not request the Server is variable substitution is enabled as that should be a user decision to substitute or not.

### 6. Implementation

For CLI, variable substitution will happen in the CLI side before making a ksqlDB request to the server. This allows less changes in ksqlDB code as we have  
the `SET CLI` to enable/disable, and variables will be living in the CLI server memory, so we have quickly access to them.

For headless, variable substitution will happen in the server side. Seems the `KsqlContext` is the right place for that.

Variable substitution will be done by using two pre-parsing steps. First, it will use the `KsqlParser.parse()` method to verify variable substitution rules.  
These rules will be defined in the `SqlBase.g4` syntax file. Once this step passes, then we'll use the `StringSubstitutor` class from Apache libraries to  
replace `{...}` variables. This library gives us easily variable replacement including nested variables, and others features we can use in the future, such as
environment variable substitution.

## Test plan

* Unit tests to validate all new methods
* Integration tests to verify different variable substitutions (negative and positive cases)

## LOEs and Delivery Milestones

This is a _small feature_. Changes are only done in a few places, so LOE is 1-2 weeks including testing.

## Documentation Updates

* Add new documentation for variable substitution, including examples

## Compatibility Implications

No compatibility implications.  
Variables will not be persisted in the command topic, so backwards and rollback compatibility are allowed.

## Security Implications

No security implications.
