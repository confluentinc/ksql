---
layout: page
title: Lexical structure data
tagline: Structure of SQL commands and statements in ksqlDB 
description: Details about SQL commands and statements in ksqlDB 
keywords: ksqldb, sql, keyword, identifier, constant, operator
---

SQL is a domain-specific language for managing and manipulating data. It’s primarily used to work with structured data, where the types and relationships across entities are well-defined. Originally adopted for relational databases, SQL is rapidly becoming the language of choice for stream processing. It’s declarative, expressive, and ubiquitous.

The American National Standards Institute (ANSI) maintains a standard for the specification of SQL. SQL-92, the third revision to the standard, is generally the most recognized form of that specification. Beyond the standard, there are many flavors and extensions to SQL so that it can express programs beyond the SQL-92 grammar.

ksqlDB’s SQL grammar was initially built around Presto’s grammar and has been judiciously extended. ksqlDB goes beyond SQL-92 because the standard currently has no constructs for streaming queries, a core aspect of this project.

## Syntax

SQL inputs are made up of a series of commands. Each command is made up of a series of tokens and ends in a semicolon (`;`). The tokens that apply depend on the command being invoked.

A token is any keyword, identifier, backticked identifier, literal, or special character. Tokens are conventionally separated by whitespace unless there is no ambiguity in the grammar. This often happens when tokens flank a special character.

As an example, the following is syntactically valid ksqlDB SQL input:

```sql
INSERT INTO s1 (a, b) VALUES ('k1', 'v1');

CREATE STREAM s2 AS
    SELECT a, b
    FROM s1
    EMIT CHANGES;

SELECT * FROM t1 WHERE k1='foo' EMIT CHANGES;
```

## Keywords

Some tokens, such as `SELECT`, `INSERT`, and `CREATE`, are known as keywords. Keywords are reserved tokens that have a specific meaning in ksqlDB’s syntax. They control their surrounding allowable tokens and execution semantics. Keywords are case insensitive, meaning `SELECT` and `select` are equivalent. You cannot create an identifier that is already a keyword (unless you use backticked identifiers).

A complete list of keywords can be found in the appendix.

The following table shows all keywords in ksqlDB SQL.


| keyword      | description                         | related keywords                         | example                                                              |
|--------------|-------------------------------------|------------------------------------------|----------------------------------------------------------------------|
| ADVANCE      | hop size in hopping WINDOW          | BY                                       | `WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 10 SECONDS)`            |
| ALL          | list hidden topics                  | SHOW                                     | `SHOW ALL TOPICS`                                                    |
| ANALYZE      |                                     |                                          |                                                                      |
| AND          | logical AND operator                | WHERE                                    | `WHERE userid<>'User_1' AND userid<>'User_2'`                        |
| ARRAY        | one-indexed array of elements       | SELECT                                   | `SELECT ARRAY[1, 2] FROM s1 EMIT CHANGES;`                           |
| AS           | alias a column, expression, or type |                                          |                                                                      |
| AT           |                                     |                                          |                                                                      |
| BEGINNING    | print from start of topic           | PRINT FROM                               | `PRINT <topic-name> FROM BEGINNING;`                                 |
| BETWEEN      | constrain a value to a range        | WHERE                                    | `SELECT event FROM events WHERE event_id BETWEEN 10 AND 20 …`        |
| BY           | specify expression                  | GROUP, ADVANCE, PARTITION                | `GROUP BY regionid`, `ADVANCE BY 10 SECONDS`, `PARTITION BY userid`  |
| CASE         | select a condition from expressions | WHEN                                     | `SELECT CASE WHEN condition THEN result [ WHEN … THEN … ] … END`     |
| CAST         | change expression type              |                                          | `SELECT id, CONCAT(CAST(COUNT(*) AS VARCHAR), '_HELLO') FROM views …`|
| CATALOG      |                                     |                                          |                                                                      |
| CHANGES      | specify push query                  | EMIT                                     | `SELECT * FROM users EMIT CHANGES;`                                  |
| COLUMN       |                                     |                                          |                                                                      |
| COLUMNS      |                                     |                                          |                                                                      |
| CONNECTOR    | manage a connector                  | CREATE, DESCRIBE, DROP                   |  `CREATE SOURCE CONNECTOR 'jdbc-connector' WITH( …`                  |
| CONNECTORS   | list all connectors                 | SHOW, LIST                               |  `SHOW CONNECTORS;`                                                  |
| CREATE       | create an object                    | STREAM, TABLE, CONNECTOR, TYPE           |  `CREATE STREAM rock_songs (artist VARCHAR, title VARCHAR) …`        |
| DATE         |                                     |                                          |                                                                      |
| DAY          | time unit of one day for a window   | WITHIN, RETENTION, SIZE                  |  `WINDOW TUMBLING (SIZE 30 SECONDS, RETENTION 1 DAY)`                |
| DAYS         | time unit of days for a window      | WITHIN, RETENTION, SIZE                  |  `WINDOW TUMBLING (SIZE 30 SECONDS, RETENTION 1000 DAYS)`            |
| DECIMAL      | decimal numeric type                |                                          |                                                                      |
| DELETE       | remove a {{ site.ak}} topic         | DROP, TOPIC                              | `DROP TABLE <table-name> DELETE TOPIC;`                              |
| DESCRIBE     | list details for an object          | STREAM, TABLE, CONNECTOR, TYPE, FUNCTION | `DESCRIBE PAGEVIEWS;`                                                |
| DISTINCT     |                                     |                                          |                                                                      |
| DROP         | delete an object                    | STREAM, TABLE, CONNECTOR, TYPE           | `DROP CONNECTOR <connector-name>;`                                   |
| ELSE         | condition in WHEN statement         | WHEN, THEN                               | `CASE WHEN units<2 THEN 'sm' WHEN units<4 THEN 'med' ELSE 'large' …` |
| EMIT         | specify push query                  | CHANGES, FINAL                           | `SELECT * FROM users EMIT CHANGES;`                                  |
| END          | close a CASE block                  | CASE                                     | `SELECT CASE WHEN condition THEN result [ WHEN … THEN … ] … END`     |
| ESCAPE       |                                     |                                          |                                                                      |
| EXISTS       | test whether object exists          | IF                                       | `DROP STREAM IF EXISTS <stream-name>;`                               |
| EXPLAIN      | show execution plan                 |                                          | `EXPLAIN <query-name>;` or `EXPLAIN <expression>;`                   |
| EXPORT       |                                     |                                          |                                                                      |
| EXTENDED     | list details for an object          | SHOW, LIST, DESCRIBE                     | `DESCRIBE EXTENDED <stream-name>;`                                   |
| FALSE        | Boolean value of FALSE              |                                          |                                                                      |
| FINAL        | specify pull query                  | CHANGES                                  | `SELECT * FROM users EMIT FINAL;`                                    |
| FROM         | specify record source for queries   | SELECT                                   | `SELECT * FROM users;`                                               |
| FULL         | specify FULL JOIN                   | OUTER JOIN                               | `CREATE TABLE t AS SELECT * FROM l FULL OUTER JOIN r ON l.ID = r.ID;`|
| FUNCTION     | list details for a function         | SHOW, LIST, DESCRIBE                     | `DESCRIBE FUNCTION <function-name>;`                                 |
| FUNCTIONS    | list all functions                  | SHOW, LIST, DESCRIBE                     | `SHOW FUNCTIONS;`                                                    |
| GRACE        | grace period for a tumbling window  | PERIOD                                   | `WINDOW TUMBLING (SIZE 1 HOUR, GRACE PERIOD 2 HOURS)`                |
| GROUP        | group rows with the same values     | BY                                       | `SELECT regionid, COUNT(*) FROM pageviews GROUP BY regionid`         |
| HAVING       | condition expression                |                                          | `GROUP BY card_number HAVING COUNT(*) > 3`                           |
| HOPPING      | specify a hopping window            | WINDOW, ADVANCE BY                       | `WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 10 SECONDS)`            |
| HOUR         | time unit of one hour for a window  | WITHIN, RETENTION, SIZE                  | `WINDOW TUMBLING (SIZE 1 HOUR, RETENTION 1 DAY)`                     |
| HOURS        | time unit of hours for a window     | WITHIN, RETENTION, SIZE                  | `WINDOW TUMBLING (SIZE 2 HOURS, RETENTION 1 DAY)`                    |
| IF           | test whether object exists          | EXISTS                                   | `DROP STREAM IF EXISTS <stream-name>;`                               |
| IN           | specify multiple values             | WHERE                                    | `WHERE name IN (value1, value2, ...)`                                |
| INNER        | specify INNER JOIN                  | JOIN                                     | `CREATE TABLE t AS SELECT * FROM l INNER JOIN r ON l.ID = r.ID;`     |
| INSERT       | insert new records in a stream/table| INTO, VALUES                             | `INSERT INTO <stream-name> ...`                                      |
| INTEGER      | integer numeric type                |                                          | `CREATE TABLE profiles (id INTEGER PRIMARY KEY, …`                   |
| INTERVAL     | number of messages to skip in PRINT | PRINT                                    | `PRINT <topic-name> INTERVAL 5;`                                     |
| INTO         | stream/table to insert values       | INSERT                                   | `INSERT INTO stream_name ...`                                        |
| IS           |                                     |                                          |                                                                      |
| JOIN         | match records in streams/tables     | LEFT, INNER, OUTER                       | `CREATE TABLE t AS SELECT * FROM l INNER JOIN r ON l.ID = r.ID;`     |
| KEY          | specify key column                  | PRIMARY                                  | `CREATE TABLE users (userId INT PRIMARY KEY, …`                      |
| LEFT         | specify LEFT JOIN                   | JOIN                                     | `CREATE TABLE t AS SELECT * FROM l LEFT JOIN r ON l.ID = r.ID;`      |
| LIKE         | match pattern                       | WHERE                                    | `WHERE UCASE(gender)='FEMALE' AND LCASE (regionid) LIKE '%_6'`       |
| LIMIT        | number of records to output         | SELECT                                   | `SELECT * FROM users EMIT CHANGES LIMIT 5;`                          |
| LIST         | list objects                        | QUERIES, STREAMS, TABLES, TYPES, …       | `SHOW STREAMS;`                                                      |
| LOAD         |                                     |                                          |                                                                      |
| MAP          | `map` data type                     | SELECT                                   | `SELECT MAP(k1:=v1, k2:=v1*2) FROM s1 EMIT CHANGES;`                 |
| MATERIALIZED |                                     |                                          |                                                                      |
| MILLISECOND  | time unit of one ms for a window    | WITHIN, RETENTION, SIZE                  | `WINDOW TUMBLING (SIZE 1 MILLISECOND, RETENTION 1 DAY)`              |
| MILLISECONDS | time unit of ms for a window        | WITHIN, RETENTION, SIZE                  | `WINDOW TUMBLING (SIZE 100 MILLISECONDS, RETENTION 1 DAY)`           |
| MINUTE       | time unit of one min for a window   | WITHIN, RETENTION, SIZE                  | `WINDOW TUMBLING (SIZE 1 MINUTE, RETENTION 1 DAY)`                   |
| MINUTES      | time unit of mins for a window      | WITHIN, RETENTION, SIZE                  | `WINDOW TUMBLING (SIZE 30 MINUTES, RETENTION 1 DAY)`                 |
| MONTH        | time unit of one month for a window | WITHIN, RETENTION, SIZE                  | `WINDOW TUMBLING (SIZE 1 HOUR, RETENTION 1 MONTH)`                   |
| MONTHS       | time unit of months for a window    | WITHIN, RETENTION, SIZE                  | `WINDOW TUMBLING (SIZE 1 HOUR, RETENTION 2 MONTHs)`                  |
| NAMESPACE    |                                     |                                          |                                                                      |
| NOT          | logical NOT operator                |                                          |                                                                      |
| NULL         | field with no value                 |                                          |                                                                      |
| ON           | specify join criteria               | JOIN                                     | `LEFT JOIN users ON pageviews.userid = users.userid`                 |
| OR           | logical OR operator                 | WHERE                                    | `WHERE userid='User_1' OR userid='User_2'`                           |
| OUTER        | specify OUTER JOIN                  | JOIN                                     | `CREATE TABLE t AS SELECT * FROM l FULL OUTER JOIN r ON l.ID = r.ID;`|
| PARTITION    | repartition a stream                | BY                                       | `PARTITION BY <key-field>`                                           |
| PARTITIONS   | partitions to distribute keys over  | CREATE                                   | `CREATE STREAM users_rekeyed WITH (PARTITIONS=6) AS …`               |
| PERIOD       | grace period for a tumbling window  | GRACE                                    | `WINDOW TUMBLING (SIZE 1 HOUR, GRACE PERIOD 2 HOURS)`                |
| PRIMARY      | specify primary key column          | KEY                                      | `CREATE TABLE users (userId INT PRIMARY KEY, …`                      |
| PRINT        | output records in a topic           | FROM BEGINNING                           | `PRINT <topic-name> FROM BEGINNING;`                                 |
| PROPERTIES   | list all properties                 | LIST, SHOW                               | `SHOW PROPERTIES;`                                                   |
| QUERIES      | list all queries                    | LIST, SHOW                               | `SHOW QUERIES;`                                                      |
| QUERY        |                                     |                                          |                                                                      |
| RENAME       |                                     |                                          |                                                                      |
| REPLACE      | string replace                      |                                          | `REPLACE(col1, 'foo', 'bar')`                                        |
| RESET        |                                     |                                          |                                                                      |
| RETENTION    | time to retain past windows         | WINDOW                                   | `WINDOW TUMBLING (SIZE 30 SECONDS, RETENTION 1000 DAYS)`             |
| RIGHT        |                                     |                                          |                                                                      |
| RUN          | execute queries from a file         | SCRIPT                                   | `RUN SCRIPT <path-to-query-file>;`                                   |
| SAMPLE       |                                     |                                          |                                                                      |
| SCRIPT       | execute queries from a file         | RUN                                      | `RUN SCRIPT <path-to-query-file>;`                                   |
| SECOND       | time unit of one sec for a window   | WITHIN, RETENTION, SIZE                  | `WINDOW TUMBLING (SIZE 1 SECOND, RETENTION 1 DAY)`                   |
| SECONDS      | time unit of secs for a window      | WITHIN, RETENTION, SIZE                  | `WINDOW TUMBLING (SIZE 30 SECONDS, RETENTION 1 DAY)`                 |
| SELECT       | query a stream or table             |                                          |                                                                      |
| SESSION      | specify a session window            | WINDOW                                   | `WINDOW SESSION (60 SECONDS)`                                        |
| SET          | assign a property value             |                                          | `SET 'auto.offset.reset'='earliest';`                                |
| SHOW         | list objects                        | QUERIES, STREAMS, TABLES, TYPES, …       | `SHOW FUNCTIONS;`                                                    |
| SINK         | create a sink connector             | CREATE CONNECTOR                         | `CREATE SINK CONNECTOR …`                                            |
| SIZE         | time length of a window             | WINDOW                                   | `WINDOW TUMBLING (SIZE 5 SECONDS)`                                   |
| SOURCE       | create a source connector           | CREATE CONNECTOR                         | `CREATE SOURCE CONNECTOR …`                                          |
| STREAM       | register a stream on a topic        | CREATE, AS SELECT                        | `CREATE STREAM users_orig AS SELECT * FROM users EMIT CHANGES;`      |
| STREAMS      | list all streams                    | LIST, SHOW                               | `SHOW STREAMS;`                                                      |
| STRUCT       | struct data type                    | SELECT                                   | `SELECT STRUCT(f1 := v1, f2 := v2) FROM s1 EMIT CHANGES;`            |
| TABLE        | register a table on a topic         | CREATE, AS SELECT                        | `CREATE TABLE users (id BIGINT PRIMARY KEY, …`                       |
| TABLES       | list all tables                     | LIST, SHOW                               | `SHOW TABLES;`                                                       |
| TERMINATE    | end a persistent query              |                                          | `TERMINATE query_id;`                                                |
| THEN         | return expression in a CASE block   | WHEN, ELSE                               | `CASE WHEN units<2 THEN 'sm' WHEN units<4 THEN 'med' ELSE 'large' …` |
| TIME         |                                     |                                          |                                                                      |
| TIMESTAMP    | specify a timestamp column          | CREATE, WITH                             | `CREATE STREAM pageviews WITH (TIMESTAMP='viewtime', …`              |
| TO           |                                     |                                          |                                                                      |
| TOPIC        | specify {{site.ak}} topic to delete | DELETE                                   | `DROP TABLE <table-name> DELETE TOPIC;`                              |
| TOPICS       | list all streams                    | LIST, SHOW                               | `SHOW TOPICS;`                                                       |
| TRUE         | Boolean value of TRUE               |                                          |                                                                      |
| TUMBLING     | specify a tumbling window           | WINDOW                                   | `WINDOW TUMBLING (SIZE 5 SECONDS)`                                   |
| TYPE         | alias a complex type declaration    | CREATE, DROP                             | `CREATE TYPE <type_name> AS <type>;`                                 |
| TYPES        | list all custom TYPE aliases        | LIST, SHOW, TYPE                         | `SHOW TYPES;`                                                        |
| UNSET        | unassign a property value           |                                          | `UNSET 'auto.offset.reset';`                                         |
| VALUES       | list of values to insert            | INSERT                                   | `INSERT INTO foo VALUES ('key', 'A');`                               |
| VIEW         |                                     |                                          |                                                                      |
| WHEN         | specify condition in a CASE block   | CASE, THEN, ELSE                         | `SELECT CASE WHEN condition THEN result [ WHEN … THEN … ] …`         |
| WHERE        | filter records by a condition       | SELECT                                   | `SELECT * FROM pageviews WHERE pageid < 'Page_20'`                   |
| WINDOW       | groups rows with the same keys      | CREATE, SELECT                           | `SELECT userid, COUNT(*) FROM users WINDOW SESSION (60 SECONDS) …`   |
| WITH         | specify object creation params      | CREATE                                   | `CREATE STREAM pageviews WITH (TIMESTAMP='viewtime', …`              |
| WITHIN       | time range in a windowed JOIN       | JOIN                                     | `SELECT * FROM impressions i JOIN clicks c WITHIN 1 minute …`        |
| YEAR         | time unit of one year for a window  | WITHIN, RETENTION, SIZE                  | `WINDOW TUMBLING (SIZE 1 HOUR, RETENTION 1 YEAR)`                    |
| YEARS        | time unit of years for a window     | WITHIN, RETENTION, SIZE                  | `WINDOW TUMBLING (SIZE 1 HOUR, RETENTION 2 YEARS)`                   |
| ZONE         |                                     |                                          |                                                                      |






## Identifiers

- Identifiers = user-space symbols
- Identify streams, tables, and other objects created by the user
  - examples: I create a stream called `s1`. `s1` is an identifier
- case insensitive

[ example ]

TODO: what makes a valid identifier?

- Backticked identifiers = escaped for exact casing
- Allows you to use any name of your choosing, including keywords.
- This is useful if you don't control the data

[ example ]

## Constants

- A value of a given type - a literal
- String constants - single quotes
- Numeric constants
- Boolean constants

## Operators

The following table lists all of the operators that are supported by ksqlDB SQL.

| operator     | meaning                        | applies to      
|--------------|--------------------------------|-----------------
| `=`          | is equal to                    | string, numeric 
| `!=` or `<>` | is not equal to                | string, numeric 
| `<`          | is less than                   | string, numeric         
| `<=`         | is less than or equal to       | string, numeric         
| `>`          | is greater than                | string, numeric         
| `>=`         | is greater than or equal to    | string, numeric         
| `+`          | addition for numeric, concatenation for string | string, numeric         
| `-`          | subtraction                    | numeric         
| `*`          | multiplication                 | numeric         
| `/`          | division                       | numeric         
| `%`          | modulus                        | numeric 
| `||` or `+`  | concatenation                  | string          
| `:=`         | assignment                     | all             
| `->`         | struct field dereference       | struct     
| `.`          | source dereference             | table, stream
| `E` or `e`   | exponent                       | numeric
| `NOT`        | logical NOT                    | boolean
| `AND`        | logical AND                    | boolean
| `OR`         | logical OR                     | boolean
| `BETWEEN`    | test if value within range     | numeric, string
| `LIKE`       | match a pattern                | string

The explanation for each operator includes a supporting example based on
the following table:

```sql
CREATE TABLE USERS (
    USERID BIGINT PRIMARY KEY,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    NICKNAMES ARRAY<STRING>,
    ADDRESS STRUCT<STREET_NAME STRING, HOUSE_NUM INTEGER>
) WITH (KAFKA_TOPIC='users', VALUE_FORMAT='AVRO');
```

### Arithmetic

You can apply the familiar arithmetic operators, like `+` and `%`, to
[numeric types](../data-types/numeric.md), like INT, BIGINT, DOUBLE, and
DECIMAL.

The following example statement uses the addition operator (`+`) to compute
the sum of the lengths of two strings:

```sql
SELECT USERID, LEN(FIRST_NAME) + LEN(LAST_NAME) AS NAME_LENGTH FROM USERS EMIT CHANGES;
```

### Concatenation

Use the concatenation operator (`+` or `||`) to concatenate
[STRING](../data-types/character.md) values.

```sql
SELECT USERID, FIRST_NAME + LAST_NAME AS FULL_NAME FROM USERS EMIT CHANGES;
```

You can use the concatenation operator for multi-part concatenation.

The following example statement uses the concatenation operator to create
an error message:

```sql
SELECT USERID,
    TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss') +
        ': :heavy_exclamation_mark: On ' +
        HOST +
        ' there were ' +
        CAST(INVALID_LOGIN_COUNT AS VARCHAR) +
        ' attempts in the last minute (threshold is >=4)'
  FROM INVALID_USERS_LOGINS_PER_HOST
  WHERE INVALID_LOGIN_COUNT>=4
  EMIT CHANGES;
```

### Source dereference

Use the source dereference operator (`.`) to specify columns by dereferencing
the source stream or table.

Providing the fully qualified column name is optional, unless the column name is
ambiguous. For example, if two sides of a join both contain a `foo` column, any
reference to `foo` in the query must be fully qualified.

The following example statement selects the values of the USERID and FIRST_NAME
columns in the USERS table by using the fully qualified column names:

```sql
SELECT USERS.USERID, USERS.FIRST_NAME FROM USERS EMIT CHANGES;
```

### Subscript

Use the subscript operator (`[subscript_expr]`) to reference the value at an
[array](data-types/compound.md#array) index or a [array](data-types/compound.md#map)
map key. Arrays indexes are one-based.

The following example statement selects the first string in the NICKNAMES array:

```sql
SELECT USERID, NICKNAMES[1] FROM USERS EMIT CHANGES;
```

### Struct dereference

Access nested data by declaring a STRUCT and using the dereference operator
(`->`) to access its fields.

The following example statement selects the values of the STREET and HOUSE_NUM
fields in the ADDRESS struct: 

```sql
SELECT USERID, ADDRESS->STREET, ADDRESS->HOUSE_NUM FROM USERS EMIT CHANGES;
```

Combine `->` with `.` to provide fully qualified names:

```sql
SELECT USERID, USERS.ADDRESS->STREET, U.ADDRESS->STREET FROM USERS U EMIT CHANGES;
```



## Special characters

Not a function/operator/identifier, have meta-meaning for the commands.

- Parens
- brackets
- commas
- semi-colon
- colon
- asterisk
- dot

## Comments

- -- double dash

TODO: does ksqlDB have multi-line comments?

There are bracketed comments in the grammar, so I think so.
'/*' .*? '*/' 

## Operator precedence

Precedence in order:

1. `*` (multiplication), `/` (division), `%` (modulus)
2. `+` (positive), - (negative), + (addition), + (concatenation), - (subtraction)
3. `=`, `>`, `<`, `>=`, `<=`, `<>`, `!=` (comparison operators)
4. NOT
5. AND
6. BETWEEN, LIKE, OR

In an expression, when two operators have the same precedence level, they're
evaluated left-to-right based on their position in the expression.

You can enclose an expression in parentheses to force precedence or clarify
precedence, for example, _(5 + 2) * 3_.