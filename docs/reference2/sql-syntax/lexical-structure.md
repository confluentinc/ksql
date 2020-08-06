---
layout: page
title: Lexical structure data
tagline: Structure of SQL commands and statements in ksqlDB 
description: Details about SQL commands and statements in ksqlDB 
keywords: ksqldb, sql, keyword, identifier, constant, operator
---

- ksqlDB SQL is SQL extended for streaming
- commands
  - tokens
  - terminate with ;
  - valid tokens
    - keyword
    - identifier
    - backticked identifier
    - special character
  - tokens separated by whitespace

[ example commands ]

- Note that it's based off Presto + has additions for other functionality.

## Keywords

- Keywords = reserved words
  - examples: select, insert, where
  - case insensitive

The following table shows all keywords in ksqlDB SQL.


| keyword      | description                         | related keywords                         | example                                                              |
|--------------|-------------------------------------|------------------------------------------|----------------------------------------------------------------------|
| ADVANCE      |                                     | BY                                       | `ADVANCE BY`                                                         |
| ALL          |                                     |                                          |                                                                      |
| ANALYZE      |                                     |                                          |                                                                      |
| AND          | logical AND operator                |                                          |                                                                      |
| ARRAY        | one-indexed array of elements       |                                          |                                                                      |
| AS           | alias a column, expression, or type |                                          |                                                                      |
| AT           |                                     |                                          |                                                                      |
| BEGINNING    |                                     | FROM                                     | `FROM BEGINNING`                                                     |
| BETWEEN      | constrain a value to a range        |                                          |                                                                      |
| BY           |                                     | GROUP, ADVANCE                           | `GROUP BY`, `ADVANCE BY`                                             |
| CASE         | select a condition from expressions | WHEN                                     |                                                                      |
| CAST         | change expression type              |                                          |                                                                      |
| CATALOG      |                                     |                                          |                                                                      |
| CHANGES      |                                     | EMIT                                     | `EMIT CHANGES`                                                       |
| COLUMN       |                                     |                                          |                                                                      |
| COLUMNS      |                                     |                                          |                                                                      |
| CONNECTOR    |                                     |                                          |                                                                      |
| CONNECTORS   |                                     |                                          |                                                                      |
| CREATE       |                                     | STREAM, TABLE, CONNECTOR, TYPE           |                                                                      |
| DATE         |                                     |                                          |                                                                      |
| DAY          |                                     |                                          |                                                                      |
| DAYS         |                                     |                                          |                                                                      |
| DECIMAL      | decimal numeric type                |                                          |                                                                      |
| DELETE       |                                     | TOPIC                                    | `[DELETE TOPIC]`                                                     |
| DESCRIBE     | list details                        | STREAM, TABLE, CONNECTOR, TYPE, FUNCTION |                                                                      |
| DISTINCT     |                                     |                                          |                                                                      |
| DROP         | delete                              | STREAM, TABLE, CONNECTOR, TYPE           |                                                                      |
| ELSE         |                                     | IF                                       |                                                                      |
| EMIT         |                                     | CHANGES                                  | `EMIT CHANGES`                                                       |
| END          |                                     | CASE                                     |                                                                      |
| ESCAPE       |                                     |                                          |                                                                      |
| EXISTS       |                                     | IF                                       | `[IF EXISTS]`                                                        |
| EXPLAIN      | show execution plan                 |                                          |                                                                      |
| EXPORT       |                                     |                                          |                                                                      |
| EXTENDED     |                                     | SHOW, LIST, DESCRIBE                     |                                                                      |
| FALSE        | Boolean value of FALSE              |                                          |                                                                      |
| FINAL        |                                     |                                          |                                                                      |
| FROM         |                                     | SELECT                                   | `SELECT * FROM users;`                                               |
| FULL         |                                     | JOIN                                     |                                                                      |
| FUNCTION     |                                     | SHOW, LIST, DESCRIBE                     |                                                                      |
| FUNCTIONS    |                                     | SHOW, LIST, DESCRIBE                     |                                                                      |
| GRACE        |                                     | PERIOD                                   | `WINDOW TUMBLING (SIZE 1 HOUR, GRACE PERIOD 2 HOURS)`                |
| GROUP        |                                     | BY                                       | `GROUP BY`                                                           |
| HAVING       | filter condition                    |                                          |                                                                      |
| HOPPING      |                                     | WINDOW                                   | `WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 10 SECONDS)`            |
| HOUR         |                                     |                                          |                                                                      |
| HOURS        |                                     |                                          |                                                                      |
| IF           |                                     |                                          |                                                                      |
| IN           | specify multiple values             | WHERE                                    | `WHERE name IN (value1, value2, ...)`                                |
| INNER        |                                     | JOIN                                     |                                                                      |
| INSERT       |                                     | INTO, VALUES                             | `INSERT INTO stream_name ...`                                        |
| INTEGER      | integer numeric type                |                                          |                                                                      |
| INTERVAL     |                                     | PRINT                                    | `PRINT topicName INTERVAL interval 5`                                |
| INTO         |                                     | INSERT                                   | `INSERT INTO stream_name ...`                                        |
| IS           |                                     |                                          |                                                                      |
| JOIN         | match records                       |                                          |                                                                      |
| KEY          |                                     | PRIMARY                                  | `userId INT PRIMARY KEY`                                             |
| LEFT         |                                     | JOIN                                     | `LEFT JOIN users ON pageviews.userid = users.userid`                 |
| LIKE         |                                     | WHERE                                    | `WHERE UCASE(gender)='FEMALE' AND LCASE (regionid) LIKE '%_6'`       |
| LIMIT        |                                     | SELECT                                   | `SELECT * FROM users EMIT CHANGES LIMIT 5;`                          |
| LIST         |                                     |                                          |                                                                      |
| LOAD         |                                     |                                          |                                                                      |
| MAP          | `map` data type                     |                                          |                                                                      |
| MATERIALIZED |                                     |                                          |                                                                      |
| MILLISECOND  |                                     |                                          |                                                                      |
| MILLISECONDS |                                     |                                          |                                                                      |
| MINUTE       |                                     |                                          |                                                                      |
| MINUTES      |                                     |                                          |                                                                      |
| MONTH        |                                     |                                          |                                                                      |
| MONTHS       |                                     |                                          |                                                                      |
| NAMESPACE    |                                     |                                          |                                                                      |
| NOT          | logical NOT operator                |                                          |                                                                      |
| NULL         |                                     |                                          |                                                                      |
| ON           |                                     | JOIN                                     | `LEFT JOIN users ON pageviews.userid = users.userid`                 |
| OR           | logical OR operator                 |                                          |                                                                      |
| OUTER        |                                     | JOIN                                     |                                                                      |
| PARTITION    |                                     | BY                                       | `PARTITION BY key_field`                                             |
| PARTITIONS   |                                     | CREATE                                   | `CREATE STREAM users_rekeyed WITH (PARTITIONS=6) AS ...`             |
| PERIOD       |                                     | GRACE                                    |                                                                      |
| PRIMARY      |                                     | KEY                                      |                                                                      |
| PRINT        |                                     |                                          |                                                                      |
| PROPERTIES   |                                     | LIST, SHOW                               | `SHOW PROPERTIES;`                                                   |
| QUERIES      |                                     | LIST, SHOW                               | `SHOW QUERIES;`                                                      |
| QUERY        |                                     |                                          |                                                                      |
| RENAME       |                                     |                                          |                                                                      |
| REPLACE      | string replace                      |                                          | `REPLACE(col1, 'foo', 'bar')`                                        |
| RESET        |                                     |                                          |                                                                      |
| RETENTION    |                                     | WINDOW                                   | `WINDOW TUMBLING (SIZE 30 SECONDS, RETENTION 1000 DAYS)`             |
| RIGHT        |                                     |                                          |                                                                      |
| RUN          |                                     | SCRIPT                                   | `RUN SCRIPT <path-to-query-file>;`                                   |
| SAMPLE       |                                     |                                          |                                                                      |
| SCRIPT       |                                     | RUN                                      | `RUN SCRIPT <path-to-query-file>;`                                   |
| SECOND       |                                     |                                          |                                                                      |
| SECONDS      |                                     |                                          |                                                                      |
| SELECT       | query a stream or table             |                                          |                                                                      |
| SESSION      |                                     | WINDOW                                   | `WINDOW SESSION (60 SECONDS)`                                        |
| SET          | assign a property value             |                                          | `SET 'auto.offset.reset'='earliest';`                                |
| SHOW         |                                     |                                          |                                                                      |
| SINK         |                                     | CREATE CONNECTOR                         | `CREATE SINK CONNECTOR ...`                                          |
| SIZE         |                                     | WINDOW                                   | `WINDOW TUMBLING (SIZE 5 SECONDS)`                                   |
| SOURCE       |                                     | CREATE CONNECTOR                         | `CREATE SOURCE CONNECTOR ...`                                        |
| STREAM       |                                     |                                          |                                                                      |
| STREAMS      |                                     | LIST, SHOW                               | `SHOW STREAMS;`                                                      |
| STRUCT       | struct data type                    |                                          |                                                                      |
| TABLE        |                                     |                                          |                                                                      |
| TABLES       |                                     | LIST, SHOW                               | `SHOW TABLES;`                                                       |
| TERMINATE    | end a persistent query              |                                          | `TERMINATE query_id;`                                                |
| THEN         |                                     | IF                                       |                                                                      |
| TIME         |                                     |                                          |                                                                      |
| TIMESTAMP    |                                     |                                          |                                                                      |
| TO           |                                     |                                          |                                                                      |
| TOPIC        |                                     |                                          |                                                                      |
| TOPICS       |                                     | LIST, SHOW                               | `SHOW TOPICS;`                                                       |
| TRUE         | Boolean value of TRUE               |                                          |                                                                      |
| TUMBLING     |                                     | WINDOW                                   | `WINDOW TUMBLING (SIZE 5 SECONDS)`                                   |
| TYPE         |                                     | CREATE, DROP                             | `CREATE TYPE <type_name> AS <type>;`                                 |
| TYPES        |                                     | LIST, SHOW                               | `SHOW TYPES;`                                                        |
| UNSET        | unassign a property value           |                                          | `UNSET 'auto.offset.reset';`                                         |
| VALUES       |                                     | INSERT                                   | `INSERT INTO foo VALUES ('key', 'A');`                               |
| VIEW         |                                     |                                          |                                                                      |
| WHEN         |                                     | CASE                                     |                                                                      |
| WHERE        |                                     | SELECT                                   | `SELECT * FROM pageviews WHERE pageid < 'Page_20'`                   |
| WINDOW       |                                     | CREATE, SELECT                           | `SELECT userid, COUNT(*) FROM users WINDOW SESSION (60 SECONDS) ...` |
| WITH         |                                     | CREATE                                   | `CREATE STREAM pageviews WITH (TIMESTAMP='viewtime', ...`            |
| WITHIN       | use with windowed JOIN              | JOIN                                     | `SELECT * FROM impressions i JOIN clicks c WITHIN 1 minute ...`      |
| YEAR         |                                     |                                          |                                                                      |
| YEARS        |                                     |                                          |                                                                      |
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
[numeric types](data-types/numeric.md), like INT, BIGINT, DOUBLE, and
DECIMAL.

The following example statement uses the addition operator (`+`) to compute
the sum of the lengths of two strings:

```sql
SELECT USERID, LEN(FIRST_NAME) + LEN(LAST_NAME) AS NAME_LENGTH FROM USERS EMIT CHANGES;
```

### Concatenation

Use the concatenation operator (`+` or `||`) to concatenate
[STRING](data-types/character.md) values.

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

### STRUCT dereference

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