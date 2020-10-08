---
layout: page
title: ksqlDB Quick Reference
tagline:  Summary of SQL syntax for ksqlDB queries and statements
description: Quick reference for SQL statements and queries in ksqlDB
keywords: ksqldb, sql, syntax, query, stream, table
---

For detailed descriptions of ksqlDB SQL statements and keywords, see the 
[ksqlDB API reference](../../ksqldb-reference).

For details on SQL syntax, see [ksqlDB syntax reference](../../syntax-reference).


## ADVANCE BY
Specify the duration of a "hop" in a HOPPING window. For more information,
see [Time and Windows in ksqlDB](../../../concepts/time-and-windows-in-ksqldb-queries).

```sql
SELECT [...], aggregate_function
  WINDOW HOPPING (SIZE <time_span> <time_units>, ADVANCE BY <time_span> <time_units>) [...]
```

## AND / OR
Logical AND/OR operators in a WHERE clause. For more information, see
[SELECT](../../ksqldb-reference/select-push-query/#example).

```sql hl_lines="4"
SELECT column_name(s)
  FROM stream_name | table_name
  WHERE condition          
    AND|OR condition
```

## AS
Alias a column, expression, or type. For more information, see
[Create a table](../../create-a-table/#create-a-ksqldb-table-from-a-ksqldb-stream).

```sql hl_lines="1"
SELECT column_name AS column_alias
  FROM stream_name | table_name
```

## BETWEEN
Constrain a value to a specified range in a WHERE clause.

```sql
WHERE expression [NOT] BETWEEN start_expression AND end_expression;            
```

The BETWEEN operator is used to indicate that a certain value must be
within a specified range, including boundaries. ksqlDB supports any
expression that resolves to a numeric or string value for comparison.

The following push query uses the BETWEEN clause to select only records
that have an `event_id` between 10 and 20.

```sql
SELECT event
  FROM events
  WHERE event_id BETWEEN 10 AND 20
  EMIT CHANGES;
```

## CASE
Select a condition from one or more expressions.

```sql
SELECT
  CASE
    WHEN condition THEN result
    [ WHEN … THEN … ]
    …
    [ WHEN … THEN … ]
    [ ELSE result ]
  END
FROM stream_name | table_name;
```

ksqlDB supports a `searched` form of CASE expression. In this form, CASE
evaluates each boolean `condition` in WHEN clauses, from left to right.
If a condition is true, CASE returns the corresponding result. If none of
the conditions is true, CASE returns the result from the ELSE clause. If
none of the conditions is true and there is no ELSE clause, CASE returns null.

The schema for all results must be the same, otherwise ksqlDB rejects the
statement.

The following example push query uses a CASE expression.

```sql
SELECT
 CASE
   WHEN orderunits < 2.0 THEN 'small'
   WHEN orderunits < 4.0 THEN 'medium'
   ELSE 'large'
 END AS case_result
FROM orders
EMIT CHANGES;
```

## CAST
Change the type of an expression to a different type.

```sql
CAST (expression AS data_type);
```

You can cast an expression's type to a new type by using CAST.

The following example query converts a numerical count, which is a BIGINT, into
a suffixed string, which is a VARCHAR. For example, the integer `5` becomes
`5_HELLO`.

```sql
SELECT page_id, CONCAT(CAST(COUNT(*) AS VARCHAR), '_HELLO')
  FROM pageviews_enriched
  WINDOW TUMBLING (SIZE 20 SECONDS)
  GROUP BY page_id;
```

## CREATE CONNECTOR
Create a new connector in the {{ site.kconnectlong }} cluster. For more
information, see [CREATE CONNECTOR](../../ksqldb-reference/create-connector).

```sql
CREATE SOURCE | SINK CONNECTOR connector_name
  WITH( property_name = expression [, ...]);
```

## CREATE STREAM
Register a stream on a {{ site.ak }} topic. For more information, see
[CREATE STREAM](../../ksqldb-reference/create-stream).

```sql
CREATE STREAM stream_name ( { column_name data_type [KEY] } [, ...] 
  WITH ( property_name = expression [, ...] );            
```

## CREATE STREAM AS SELECT
Create a new materialized stream and corresponding {{ site.ak }} topic, and
stream the result of the query into the topic. For more information, see
[CREATE STREAM AS SELECT](../../ksqldb-reference/create-stream-as-select).

```sql
CREATE STREAM stream_name
  [WITH ( property_name = expression [, ...] )]
  AS SELECT  select_expr [, ...]
  FROM from_stream
  [[ LEFT | FULL | INNER ] JOIN [join_table | join_stream]
    [ WITHIN [(before TIMEUNIT, after TIMEUNIT) | N TIMEUNIT] ]
    ON join_criteria]* 
  [ WHERE condition ]
  [PARTITION BY column_name]
  EMIT CHANGES;
```

## CREATE TABLE
Register a stream on a {{ site.ak }} topic. For more information, see
[CREATE TABLE](../../ksqldb-reference/create-table).

```sql
CREATE TABLE table_name ( { column_name data_type (PRIMARY KEY) } [, ...] )
  WITH ( property_name = expression [, ...] );
```

## CREATE TABLE AS SELECT
Create a new materialized table and corresponding {{ site.ak }} topic, and
stream the result of the query as a changelog into the topic. For more
information, see [CREATE TABLE AS SELECT](../../ksqldb-reference/create-table-as-select). 

```sql
CREATE TABLE table_name
  [WITH ( property_name = expression [, ...] )]
  AS SELECT  select_expr [, ...]
  FROM from_stream | from_table
  [[ LEFT | FULL | INNER ] JOIN [join_table | join_stream] ON join_criteria]* 
  [ WINDOW window_expression ]
  [ WHERE condition ]
  [ GROUP BY grouping_expression ]
  [ HAVING having_expression ]
  EMIT CHANGES;
```

## CREATE TYPE
Alias a complex type declaration. For more information, see
[CREATE TYPE](../../ksqldb-reference/create-type).

```sql
CREATE TYPE <type_name> AS <type>;
```

## DESCRIBE
List columns in a stream or table along with their data types and other
attributes. For more information, see [DESCRIBE](../../ksqldb-reference/describe).

```sql
DESCRIBE [EXTENDED] (stream_name | table_name);
```

## DESCRIBE CONNECTOR
List details about a connector. For more information, see
[DESCRIBE CONNECTOR](../../ksqldb-reference/describe-connector).

```sql
DESCRIBE CONNECTOR connector_name;
```

## DESCRIBE FUNCTION
List details about a function, including input parameters and return type.
For more information, see [DESCRIBE FUNCTION](../../ksqldb-reference/describe-function).

```sql
DESCRIBE FUNCTION function_name;
```

## DROP CONNECTOR
Delete a connector from the {{ site.kconnect }} cluster. For more information,
see [DROP CONNECTOR](../../ksqldb-reference/drop-connector).

```sql
DROP CONNECTOR connector_name;
```

## DROP STREAM
Drop an existing stream and optionally mark the stream's source topic for
deletion. For more information, see [DROP STREAM](../../ksqldb-reference/drop-stream).

```sql
DROP STREAM [IF EXISTS] stream_name [DELETE TOPIC];
```

## DROP TABLE
Drop an existing table and optionally mark the table's source topic for
deletion. For more information, see [DROP TABLE](../../ksqldb-reference/drop-table).

```sql
DROP TABLE [IF EXISTS] table_name [DELETE TOPIC];
```

## DROP TYPE
Remove a type alias from ksqlDB. For more information, see
[DROP TYPE](../../ksqldb-reference/drop-type).

```sql
DROP TYPE [IF EXISTS] <type_name> AS <type>;
```

## EMIT CHANGES
Specify a push query with a continuous output refinement in a SELECT statement. 
For more information, see [Push Queries](../../concepts/queries/push).

```sql
CREATE STREAM stream_name
  AS SELECT  select_expr [, ...]
  FROM from_stream
  EMIT CHANGES;
```

## EMIT FINAL
Specify a push query with a suppressed output refinement in a SELECT statement on a 
windowed aggregation. 
For more information, see [Push Queries](../../concepts/queries/push).

```sql
CREATE TABLE table_name
  AS SELECT  select_expr_with_aggregation [, ...]
  FROM from_stream
  [ WINDOW window_expression ]
  [ GROUP BY grouping_expression ]
  EMIT FINAL;
```

## EXPLAIN
Show the execution plan for a SQL expression or running query. For more
information, see [EXPLAIN](../../ksqldb-reference/explain).

```sql
EXPLAIN (sql_expression | query_id);
```

## FULL JOIN
Select all records when there is a match in the left stream/table _or_ the
right stream/table records. Equivalent to FULL OUTER JOIN. For more information,
see [Join streams and tables](../joins/join-streams-and-tables).

```sql hl_lines="3"
SELECT column_name(s)
  FROM stream_name1 | table_name1
   FULL JOIN stream_name2 | table_name2
   ON <stream_name1|table_name1>.column_name=<stream_name2|table_name2>.column_name
```

## GROUP BY
Group records in a window. Required by the WINDOW clause. For more information,
see [Time and Windows in ksqlDB](../../concepts/time-and-windows-in-ksqldb-queries).

```sql hl_lines="4"
SELECT column_name, aggregate_function(column_name)
  FROM table_name
  WHERE column_name operator value
  GROUP BY column_name
```

## HAVING
Extract records from an aggregation that fulfill a specified condition.

```sql hl_lines="5"
SELECT column_name, aggregate_function(column_name)
  FROM table_name
  WHERE column_name operator value
  GROUP BY column_name
  HAVING aggregate_function(column_name) operator value
```

## HOPPING
Group input records into fixed-sized, possibly overlapping windows,
based on the timestamps of the records. For more information, see
[HOPPING](../../ksqldb-reference/select-push-query/#hopping-window).

```sql hl_lines="3"
SELECT WINDOWSTART, WINDOWEND, aggregate_function
  FROM from_stream
  WINDOW HOPPING window_expression
  EMIT CHANGES;
```

## IF EXISTS
Test whether a stream or table is present in ksqlDB.

```sql
DROP STREAM [IF EXISTS] stream_name [DELETE TOPIC];
DROP TABLE  [IF EXISTS] table_name  [DELETE TOPIC];
```

## IN
Specify multiple values in a WHERE clause.

```sql hl_lines="4"
SELECT column_name(s)
  FROM stream_name | table_name
  WHERE column_name
  IN (value1,value2,..)
```

## INNER JOIN
Select records in a stream or table that have matching values in another stream
or table. For more information, see
[Join streams and tables](../joins/join-streams-and-tables).

```sql hl_lines="3"
SELECT column_name(s)
  FROM stream_name1 | table_name1
   INNER JOIN stream_name2 | table_name2
   ON <stream_name1|table_name1>.column_name=<stream_name2|table_name2>.column_name
```

## INSERT INTO
Stream the result of a SELECT query into an existing stream and its underlying
{{ site.ak }} topic. For more information, see [INSERT INTO](../../ksqldb-reference/insert-into).

```sql
INSERT INTO stream_name
  SELECT select_expr [., ...]
  FROM from_stream
  [ LEFT | FULL | INNER ] JOIN [join_table | join_stream]
    [ WITHIN [(before TIMEUNIT, after TIMEUNIT) | N TIMEUNIT] ]
    ON join_criteria
  [ WHERE condition ]
  [ PARTITION BY column_name ]
  EMIT CHANGES;
```

## INSERT VALUES
Produce a row into an existing stream or table and its underlying {{ site.ak }}
topic based on explicitly specified values. For more information, see
[INSERT VALUES](../../ksqldb-reference/insert-values).

```sql
INSERT INTO stream_name|table_name [(column_name [, ...]])]
  VALUES (value [,...]);
```

## LEFT JOIN
Select all records from the left stream/table and the matched records from the
right stream/table. For more information, see
[Join streams and tables](../joins/join-streams-and-tables).

```sql hl_lines="3"
SELECT column_name(s)
  FROM stream_name1 | table_name1
   LEFT JOIN stream_name2 | table_name2
   ON <stream_name1|table_name1>.column_name=<stream_name2|table_name2>.column_name
```

## LIKE
Match a string with the specified pattern.

```sql hl_lines="3"
  SELECT select_expr [., ...]
    FROM from_stream | from_table
    WHERE condition LIKE pattern_string;
```

The LIKE operator is used for prefix or suffix matching. ksqlDB supports
the `%` wildcard, which represents zero or more characters.

The following push query uses the `%` wildcard to match any `user_id` that
starts with "santa".

```sql
SELECT user_id
  FROM users
  WHERE user_id LIKE 'santa%'
  EMIT CHANGES;
```

## PARTITION BY
Repartition a stream. For more information, see
[Partition Data to Enable Joins](../joins/partition-data).

```sql hl_lines="6"
CREATE STREAM stream_name
  WITH ([...,]
        PARTITIONS=number_of_partitions)
  AS SELECT select_expr [., ...]
  FROM from_stream
  PARTITION BY key_field
  EMIT CHANGES;
```

## PRINT
Print the contents of {{ site.ak }} topics to the ksqlDB CLI. For more
information, see [PRINT](../../ksqldb-reference/print).

```sql
PRINT topicName [FROM BEGINNING] [INTERVAL interval] [LIMIT limit]
```

## RUN SCRIPT
Execute predefined queries and commands from a file. For more
information, see [RUN SCRIPT](../../ksqldb-reference/run-script).

```sql
RUN SCRIPT <path-to-query-file>;
```

## SELECT (Pull Query)
Pull the current value from a materialized table and terminate. For more
information, see [SELECT (Pull Query)](../../ksqldb-reference/select-pull-query).

```sql
SELECT select_expr [, ...]
  FROM aggregate_table
  WHERE key_column=key
  [AND window_bounds];
```

## SELECT (Push Query)
Push a continuous stream of updates to a stream or table. For more
information, see [SELECT (Push Query)](../../ksqldb-reference/select-push-query).

```sql
SELECT select_expr [, ...]
  FROM from_item
  [ LEFT JOIN join_table ON join_criteria ]
  [ WINDOW window_expression ]
  [ WHERE condition ]
  [ GROUP BY grouping_expression ]
  [ HAVING having_expression ]
  EMIT CHANGES
  [ LIMIT count ];
```

## SESSION
Group input records into a session window. For more information, see
[SELECT (Push Query)](../../ksqldb-reference/select-push-query/#session-window).

```sql hl_lines="3"
SELECT WINDOWSTART, WINDOWEND, aggregate_function
  FROM from_stream
  WINDOW SESSION window_expression
  EMIT CHANGES;
```

## SHOW CONNECTORS
List all connectors in the {{ site.kconnect }} cluster. For more information,
see [SHOW CONNECTORS](../../ksqldb-reference/show-connectors).

```sql
SHOW | LIST CONNECTORS;
```

## SHOW FUNCTIONS
List available scalar and aggregate functions available. For more information,
see [SHOW FUNCTIONS](../../ksqldb-reference/show-functions).

```sql
SHOW | LIST FUNCTIONS;
```

## SHOW PROPERTIES
List the [configuration settings](../../operate-and-deploy/installation/server-config/config-reference.md)
that are currently in effect. For more information, see [SHOW PROPERTIES](../../ksqldb-reference/show-properties).

```sql
SHOW PROPERTIES;
```

## SHOW QUERIES
List queries that are currently running in the cluster. For more information,
see [SHOW QUERIES](../../ksqldb-reference/show-queries).

```sql
SHOW | LIST QUERIES [EXTENDED];
```

## SHOW STREAMS
List the currently defined streams. For more information,
see [SHOW STREAMS](../../ksqldb-reference/show-streams).

```sql
SHOW | LIST STREAMS [EXTENDED];
```

## SHOW TABLES
List the currently defined tables. For more information,
see [SHOW TABLES](../../ksqldb-reference/show-tables).

```sql
SHOW | LIST TABLES [EXTENDED];
```

## SHOW TOPICS
List the available topics in the {{ site.ak }} cluster that ksqlDB is
configured to connect to. For more information, see
[SHOW TOPICS](../../ksqldb-reference/show-topics).

```sql
SHOW | LIST [ALL] TOPICS [EXTENDED];
```

## SHOW TYPES
List all custom types and their type definitions. For more information,
see [SHOW TYPES](../../ksqldb-reference/show-types).

```sql
SHOW | LIST TYPES;
```

## SIZE
Specify the duration of a HOPPING or TUMBLING window. For more information,
see [Time and Windows in ksqlDB](../../concepts/time-and-windows-in-ksqldb-queries).

```sql hl_lines="3"
SELECT WINDOWSTART, WINDOWEND, aggregate_function
  FROM from_stream
  WINDOW TUMBLING (SIZE <time_span> <time_units>)
  EMIT CHANGES;
```

## SPOOL
Store issued commands and their results in a file. For more information,
see [SPOOL](../../ksqldb-reference/show-spool).

```sql
SPOOL <file_name|OFF>
```

## TERMINATE
End a persistent query. For more information, see [SPOOL](../../ksqldb-reference/terminate).

```sql
TERMINATE query_id;
```

## TUMBLING
Group input records into fixed-sized, non-overlapping windows based on the
timestamps of the records. For more information, see
[TUMBLING](../../ksqldb-reference/select-push-query/#tumbling-window).

```sql hl_lines="3"
SELECT WINDOWSTART, WINDOWEND, aggregate_function
  FROM from_stream
  WINDOW TUMBLING window_expression
  EMIT CHANGES;
```

## WHERE
Extract records that fulfill a specified condition. For more information, see
[SELECT](../../ksqldb-reference/select-push-query/#example).  

```sql hl_lines="3"
SELECT column_name(s)
  FROM from_stream | from_table
  WHERE column_name operator value
```

## WINDOW
Group input records that have the same key into a window, for operations like
aggregations and joins. For more information, see
[WINDOW](../../ksqldb-reference/select-push-query/#window).

```sql hl_lines="3"
SELECT WINDOWSTART, WINDOWEND, aggregate_function
  FROM from_stream
  WINDOW window_expression
  EMIT CHANGES;
```

## WINDOWSTART / WINDOWEND
Specify the beginning and end bounds a window. For more information, see
[WINDOW](../../ksqldb-reference/select-push-query/#window).

```sql hl_lines="1"
SELECT WINDOWSTART, WINDOWEND, aggregate_function
  FROM from_stream
  WINDOW window_expression
  EMIT CHANGES;
```