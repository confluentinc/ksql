---
layout: page
title: ksqlDB SQL keywords and operators
tagline: SQL language keywords
description: Tables listing all valid keywords and operators in ksqlDB SQL
keywords: ksqldb, sql, keyword, operators
---

## Keywords

The following table shows all keywords in the language.

| keyword      | description                             | example                                                              |
|--------------|-----------------------------------------|----------------------------------------------------------------------|
| `ADVANCE`      | hop size in hopping window            | `WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 10 SECONDS)`            |
| `ALL`          | list hidden topics                    | `SHOW ALL TOPICS`                                                    |
| `AND`          | logical "and" operator                | `WHERE userid<>'User_1' AND userid<>'User_2'`                        |
| `ARRAY`        | one-indexed array of elements         | `SELECT ARRAY[1, 2] FROM s1 EMIT CHANGES;`                           |
| `AS`           | alias a column, expression, or type   |                                                                      |
| `BEGINNING`    | print from start of topic             | `PRINT <topic-name> FROM BEGINNING;`                                 |
| `BETWEEN`      | constrain a value to a range          | `SELECT event FROM events WHERE event_id BETWEEN 10 AND 20 …`        |
| `BY`           | specify expression                    | `GROUP BY regionid`, `ADVANCE BY 10 SECONDS`, `PARTITION BY userid`  |
| `CASE`         | select a condition from expressions   | `SELECT CASE WHEN condition THEN result [ WHEN … THEN … ] … END`     |
| `CAST`         | change expression type                | `SELECT id, CONCAT(CAST(COUNT(*) AS VARCHAR), '_HELLO') FROM views …`|
| `CHANGES`      | specify incremental refinement type   | `SELECT * FROM users EMIT CHANGES;`                                  |
| `CONNECTOR`    | manage a connector                    |  `CREATE SOURCE CONNECTOR 'jdbc-connector' WITH( …`                  |
| `CONNECTORS`   | list all connectors                   |  `SHOW CONNECTORS;`                                                  |
| `CREATE`       | create an object                      |  `CREATE STREAM rock_songs (artist VARCHAR, title VARCHAR) …`        |
| `DAY`          | time unit of one day for a window     |  `WINDOW TUMBLING (SIZE 30 SECONDS, RETENTION 1 DAY)`                |
| `DAYS`         | time unit of days for a window        |  `WINDOW TUMBLING (SIZE 30 SECONDS, RETENTION 1000 DAYS)`            |
| `DECIMAL`      | decimal numeric type                  |                                                                      |
| `DELETE`       | remove a {{ site.ak}} topic           | `DROP TABLE <table-name> DELETE TOPIC;`                              |
| `DESCRIBE`     | list details for an object            | `DESCRIBE PAGEVIEWS;`                                                |
| `DROP`         | delete an object                      | `DROP CONNECTOR <connector-name>;`                                   |
| `ELSE`         | condition in `WHEN` statement         | `CASE WHEN units<2 THEN 'sm' WHEN units<4 THEN 'med' ELSE 'large' …` |
| `EMIT`         | specify push query                    | `SELECT * FROM users EMIT CHANGES;`                                  |
| `END`          | close a `CASE` block                  | `SELECT CASE WHEN condition THEN result [ WHEN … THEN … ] … END`     |
| `EXISTS`       | test whether object exists            | `DROP STREAM IF EXISTS <stream-name>;`                               |
| `EXPLAIN`      | show execution plan                   | `EXPLAIN <query-name>;` or `EXPLAIN <expression>;`                   |
| `EXTENDED`     | list details for an object            | `DESCRIBE EXTENDED <stream-name>;`                                   |
| `FALSE`        | Boolean value of false                |                                                                      |
| `FINAL`        | specify pull query                    | `SELECT * FROM users EMIT FINAL;`                                    |
| `FROM`         | specify record source for queries     | `SELECT * FROM users;`                                               |
| `FULL`         | specify `FULL JOIN`                   | `CREATE TABLE t AS SELECT * FROM l FULL OUTER JOIN r ON l.ID = r.ID;`|
| `FUNCTION`     | list details for a function           | `DESCRIBE FUNCTION <function-name>;`                                 |
| `FUNCTIONS`    | list all functions                    | `SHOW FUNCTIONS;`                                                    |
| `GRACE`        | grace period for a tumbling window    | `WINDOW TUMBLING (SIZE 1 HOUR, GRACE PERIOD 2 HOURS)`                |
| `GROUP`        | group rows with the same values       | `SELECT regionid, COUNT(*) FROM pageviews GROUP BY regionid`         |
| `HAVING`       | condition expression                  | `GROUP BY card_number HAVING COUNT(*) > 3`                           |
| `HOPPING`      | specify a hopping window              | `WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 10 SECONDS)`            |
| `HOUR`         | time unit of one hour for a window    | `WINDOW TUMBLING (SIZE 1 HOUR, RETENTION 1 DAY)`                     |
| `HOURS`        | time unit of hours for a window       | `WINDOW TUMBLING (SIZE 2 HOURS, RETENTION 1 DAY)`                    |
| `IF`           | test whether object exists            | `DROP STREAM IF EXISTS <stream-name>;`                               |
| `IN`           | specify multiple values               | `WHERE name IN (value1, value2, ...)`                                |
| `INNER`        | specify `INNER JOIN`                  | `CREATE TABLE t AS SELECT * FROM l INNER JOIN r ON l.ID = r.ID;`     |
| `INSERT`       | insert new records in a stream/table  | `INSERT INTO <stream-name> ...`                                      |
| `INTEGER`      | integer numeric type                  | `CREATE TABLE profiles (id INTEGER PRIMARY KEY, …`                   |
| `INTERVAL`     | number of messages to skip in `PRINT` | `PRINT <topic-name> INTERVAL 5;`                                     |
| `INTO`         | stream/table to insert values         | `INSERT INTO stream_name ...`                                        |
| `IS`           |                                       |                                                                      |
| `JOIN`         | match records in streams/tables       | `CREATE TABLE t AS SELECT * FROM l INNER JOIN r ON l.ID = r.ID;`     |
| `KEY`          | specify key column                    | `CREATE TABLE users (userId INT PRIMARY KEY, …`                      |
| `LEFT`         | specify `LEFT JOIN`                   | `CREATE TABLE t AS SELECT * FROM l LEFT JOIN r ON l.ID = r.ID;`      |
| `LIKE`         | match pattern                         | `WHERE UCASE(gender)='FEMALE' AND LCASE (regionid) LIKE '%_6'`       |
| `LIMIT`        | number of records to output           | `SELECT * FROM users EMIT CHANGES LIMIT 5;`                          |
| `LIST`         | list objects                          | `SHOW STREAMS;`                                                      |
| `MAP`          | `map` data type                       | `SELECT MAP(k1:=v1, k2:=v1*2) FROM s1 EMIT CHANGES;`                 |
| `MILLISECOND`  | time unit of one ms for a window      | `WINDOW TUMBLING (SIZE 1 MILLISECOND, RETENTION 1 DAY)`              |
| `MILLISECONDS` | time unit of ms for a window          | `WINDOW TUMBLING (SIZE 100 MILLISECONDS, RETENTION 1 DAY)`           |
| `MINUTE`       | time unit of one min for a window     | `WINDOW TUMBLING (SIZE 1 MINUTE, RETENTION 1 DAY)`                   |
| `MINUTES`      | time unit of mins for a window        | `WINDOW TUMBLING (SIZE 30 MINUTES, RETENTION 1 DAY)`                 |
| `MONTH`        | time unit of one month for a window   | `WINDOW TUMBLING (SIZE 1 HOUR, RETENTION 1 MONTH)`                   |
| `MONTHS`       | time unit of months for a window      | `WINDOW TUMBLING (SIZE 1 HOUR, RETENTION 2 MONTHs)`                  |
| `NOT`          | logical "not" operator                |                                                                      |
| `NULL`         | field with no value                   |                                                                      |
| `ON`           | specify join criteria                 | `LEFT JOIN users ON pageviews.userid = users.userid`                 |
| `OR`           | logical "or" operator                 | `WHERE userid='User_1' OR userid='User_2'`                           |
| `OUTER`        | specify `OUTER JOIN`                  | `CREATE TABLE t AS SELECT * FROM l FULL OUTER JOIN r ON l.ID = r.ID;`|
| `PARTITION`    | repartition a stream                  | `PARTITION BY <key-field>`                                           |
| `PARTITIONS`   | partitions to distribute keys over    | `CREATE STREAM users_rekeyed WITH (PARTITIONS=6) AS …`               |
| `PERIOD`       | grace period for a tumbling window    | `WINDOW TUMBLING (SIZE 1 HOUR, GRACE PERIOD 2 HOURS)`                |
| `PRIMARY`      | specify primary key column            | `CREATE TABLE users (userId INT PRIMARY KEY, …`                      |
| `PRINT`        | output records in a topic             | `PRINT <topic-name> FROM BEGINNING;`                                 |
| `PROPERTIES`   | list all properties                   | `SHOW PROPERTIES;`                                                   |
| `QUERIES`      | list all queries                      | `SHOW QUERIES;`                                                      |
| `REPLACE`      | string replace                        | `REPLACE(col1, 'foo', 'bar')`                                        |
| `RETENTION`    | time to retain past windows           | `WINDOW TUMBLING (SIZE 30 SECONDS, RETENTION 1000 DAYS)`             |
| `RIGHT`        |                                       |                                                                      |
| `RUN`          | execute queries from a file           | `RUN SCRIPT <path-to-query-file>;`                                   |
| `SCRIPT`       | execute queries from a file           | `RUN SCRIPT <path-to-query-file>;`                                   |
| `SECOND`       | time unit of one sec for a window     | `WINDOW TUMBLING (SIZE 1 SECOND, RETENTION 1 DAY)`                   |
| `SECONDS`      | time unit of secs for a window        | `WINDOW TUMBLING (SIZE 30 SECONDS, RETENTION 1 DAY)`                 |
| `SELECT`       | query a stream or table               |                                                                      |
| `SESSION`      | specify a session window              | `WINDOW SESSION (60 SECONDS)`                                        |
| `SET`          | assign a property value               | `SET 'auto.offset.reset'='earliest';`                                |
| `SHOW`         | list objects                          | `SHOW FUNCTIONS;`                                                    |
| `SINK`         | create a sink connector               | `CREATE SINK CONNECTOR …`                                            |
| `SIZE`         | time length of a window               | `WINDOW TUMBLING (SIZE 5 SECONDS)`                                   |
| `SOURCE`       | create a source connector             | `CREATE SOURCE CONNECTOR …`                                          |
| `STREAM`       | register a stream on a topic          | `CREATE STREAM users_orig AS SELECT * FROM users EMIT CHANGES;`      |
| `STREAMS`      | list all streams                      | `SHOW STREAMS;`                                                      |
| `STRUCT`       | struct data type                      | `SELECT STRUCT(f1 := v1, f2 := v2) FROM s1 EMIT CHANGES;`            |
| `TABLE`        | register a table on a topic           | `CREATE TABLE users (id BIGINT PRIMARY KEY, …`                       |
| `TABLES`       | list all tables                       | `SHOW TABLES;`                                                       |
| `TERMINATE`    | end a persistent query                | `TERMINATE query_id;`                                                |
| `THEN`         | return expression in a CASE block     | `CASE WHEN units<2 THEN 'sm' WHEN units<4 THEN 'med' ELSE 'large' …` |
| `TIMESTAMP`    | specify a timestamp column            | `CREATE STREAM pageviews WITH (TIMESTAMP='viewtime', …`              |
| `TOPIC`        | specify {{site.ak}} topic to delete   | `DROP TABLE <table-name> DELETE TOPIC;`                              |
| `TOPICS`       | list all streams                      | `SHOW TOPICS;`                                                       |
| `TRUE`         | Boolean value of true                 |                                                                      |
| `TUMBLING`     | specify a tumbling window             | `WINDOW TUMBLING (SIZE 5 SECONDS)`                                   |
| `TYPE`         | alias a complex type declaration      | `CREATE TYPE <type_name> AS <type>;`                                 |
| `TYPES`        | list all custom type aliases          | `SHOW TYPES;`                                                        |
| `UNSET`        | unassign a property value             | `UNSET 'auto.offset.reset';`                                         |
| `VALUES`       | list of values to insert              | `INSERT INTO foo VALUES ('key', 'A');`                               |
| `WHEN`         | specify condition in a `CASE` block   | `SELECT CASE WHEN condition THEN result [ WHEN … THEN … ] …`         |
| `WHERE`        | filter records by a condition         | `SELECT * FROM pageviews WHERE pageid < 'Page_20'`                   |
| `WINDOW`       | groups rows with the same keys        | `SELECT userid, COUNT(*) FROM users WINDOW SESSION (60 SECONDS) …`   |
| `WITH`         | specify object creation params        | `CREATE STREAM pageviews WITH (TIMESTAMP='viewtime', …`              |
| `WITHIN`       | time range in a windowed join         | `SELECT * FROM impressions i JOIN clicks c WITHIN 1 minute …`        |
| `YEAR`         | time unit of one year for a window    | `WINDOW TUMBLING (SIZE 1 HOUR, RETENTION 1 YEAR)`                    |
| `YEARS`        | time unit of years for a window       | `WINDOW TUMBLING (SIZE 1 HOUR, RETENTION 2 YEARS)`                   |

## Operators

The following table shows all operators in the language.

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