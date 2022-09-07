--@test: null - stream clone row with null value
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (ID) VALUES (1);
INSERT INTO `INPUT` (ID, NAME) VALUES (2, NULL);
ASSERT VALUES `OUTPUT` (ID) VALUES (1);
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (2, NULL);

--@test: null - stream filter with null value
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE NAME IS NULL;
INSERT INTO `INPUT` (ID) VALUES (1);
INSERT INTO `INPUT` (ID, NAME) VALUES (2, NULL);
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (2, NULL);

--@test: null - stream PARTITION BY with null value
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT PARTITION BY NAME;
INSERT INTO `INPUT` (ID) VALUES (1);
INSERT INTO `INPUT` (ID, NAME) VALUES (2, NULL);
INSERT INTO `INPUT` (ID, NAME) VALUES (3, 'n');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (1, NULL);
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (2, NULL);
ASSERT VALUES `OUTPUT` (NAME, ID) VALUES ('n', 3);

--@test: null - stream PARTITION BY IS NULL with null value
CREATE STREAM INPUT (ID INT KEY, NAME STRING, VAL STRING) WITH (kafka_topic='test_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT PARTITION BY (NAME IS NULL);
INSERT INTO `INPUT` (ID, ROWTIME) VALUES (1, 1);
INSERT INTO `INPUT` (ID, NAME, VAL, ROWTIME) VALUES (2, NULL, 'a', 2);
INSERT INTO `INPUT` (ID, NAME, VAL, ROWTIME) VALUES (3, 'n', 'b', 3);
ASSERT VALUES `OUTPUT` (ROWTIME) VALUES (1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, NAME, ID, VAL, ROWTIME) VALUES (true, NULL, 2, 'a', 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, NAME, ID, VAL, ROWTIME) VALUES (false, 'n', 3, 'b', 3);

--@test: null - stream GROUP BY with null value
CREATE STREAM INPUT (NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT NAME, COUNT() AS COUNT FROM INPUT GROUP BY NAME;
INSERT INTO `INPUT` (ROWTIME) VALUES (1);
INSERT INTO `INPUT` (NAME, ROWTIME) VALUES (NULL, 2);
INSERT INTO `INPUT` (NAME, ROWTIME) VALUES ('n', 3);
ASSERT VALUES `OUTPUT` (NAME, COUNT, ROWTIME) VALUES ('n', 1, 3);

--@test: null - stream GROUP BY IS NULL with null value
CREATE STREAM INPUT (NAME STRING) WITH (kafka_topic='test_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT NAME IS NULL, COUNT() AS COUNT FROM INPUT GROUP BY (NAME IS NULL);
INSERT INTO `INPUT` (ROWTIME) VALUES (1);
INSERT INTO `INPUT` (NAME, ROWTIME) VALUES (NULL, 2);
INSERT INTO `INPUT` (NAME, ROWTIME) VALUES ('n', 3);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, COUNT, ROWTIME) VALUES (true, 1, 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, COUNT, ROWTIME) VALUES (false, 1, 3);

--@test: null - is null
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, ID IS NULL AS ID_NULL, NAME IS NULL AS NAME_NULL FROM INPUT;
INSERT INTO `INPUT` (ID, NAME) VALUES (1, 'not null');
INSERT INTO `INPUT` (NAME) VALUES (NULL);
INSERT INTO `INPUT` (ID) VALUES (0);
ASSERT VALUES `OUTPUT` (ID, ID_NULL, NAME_NULL) VALUES (1, false, false);
ASSERT VALUES `OUTPUT` (ID_NULL, NAME_NULL) VALUES (true, true);
ASSERT VALUES `OUTPUT` (ID, ID_NULL, NAME_NULL) VALUES (0, false, true);

--@test: null - is not null
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, ID IS NOT NULL AS ID_NULL, NAME IS NOT NULL AS NAME_NULL FROM INPUT;
INSERT INTO `INPUT` (ID, NAME) VALUES (1, 'not null');
INSERT INTO `INPUT` (NAME) VALUES (NULL);
INSERT INTO `INPUT` (ID) VALUES (0);
ASSERT VALUES `OUTPUT` (ID, ID_NULL, NAME_NULL) VALUES (1, true, true);
ASSERT VALUES `OUTPUT` (ID_NULL, NAME_NULL) VALUES (false, false);
ASSERT VALUES `OUTPUT` (ID, ID_NULL, NAME_NULL) VALUES (0, true, false);

--@test: null - null equals
CREATE STREAM INPUT (ID INT KEY, COL0 BIGINT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, ID = COL0, NULL IS NULL FROM INPUT;
INSERT INTO `INPUT` (COL0) VALUES (12344);
INSERT INTO `INPUT` (COL0) VALUES (NULL);
INSERT INTO `INPUT` (ID) VALUES (0);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (false, true);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (false, true);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0, KSQL_COL_1) VALUES (0, false, true);

--@test: null - comparison with null
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Comparison with NULL not supported: NULL <> NULL
CREATE STREAM INPUT (ID INT KEY, COL0 BIGINT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, NULL <> NULL FROM INPUT;

--@test: null - coalesce
CREATE STREAM INPUT (COL0 INT KEY, COL1 INT, COL2 STRING, COL3 ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT COL0, COALESCE(COL0, COL1, COL0, COL1) AS A, COALESCE(COL2, 'x') AS B, COALESCE(COL3, ARRAY[10, 20]) AS C FROM INPUT;
INSERT INTO `INPUT` (COL0, COL1, COL2, COL3) VALUES (1, 2, 'not null', ARRAY[1, 2, 3]);
INSERT INTO `INPUT` (COL0) VALUES (NULL);
INSERT INTO `INPUT` (COL0) VALUES (NULL);
INSERT INTO `INPUT` (COL1, COL2, COL3) VALUES (2, 'not null', ARRAY[4, 5, 6]);
ASSERT VALUES `OUTPUT` (COL0, A, B, C) VALUES (1, 1, 'not null', ARRAY[1, 2, 3]);
ASSERT VALUES `OUTPUT` (A, B, C) VALUES (NULL, 'x', ARRAY[10, 20]);
ASSERT VALUES `OUTPUT` (COL0) VALUES (NULL);
ASSERT VALUES `OUTPUT` (A, B, C) VALUES (2, 'not null', ARRAY[4, 5, 6]);

--@test: null - coalesce - no params
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Function 'COALESCE' does not accept parameters ()
CREATE STREAM INPUT (COL0 INT KEY, COL1 STRING, COL2 ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT COL0, COALESCE() FROM INPUT;
--@test: null - coalesce - with parse_date
CREATE STREAM INPUT (date_string VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT parse_date(date_string, 'yyyy-MM-dd') as parse_as_date, parse_date(concat(date_string, '-01-01'), 'yyyy-MM-dd') as parse_as_year, coalesce(parse_date(date_string, 'yyyy-MM-dd'),  parse_date(concat(date_string, '-01-01'), 'yyyy-MM-dd')) as parse_as_either FROM INPUT;
INSERT INTO `INPUT` (date_string) VALUES ('2003-12-24');
INSERT INTO `INPUT` (date_string) VALUES ('2008-08-13');
INSERT INTO `INPUT` (date_string) VALUES ('2004');
INSERT INTO `INPUT` (date_string) VALUES ('2012');
ASSERT VALUES `OUTPUT` (PARSE_AS_DATE, PARSE_AS_YEAR, PARSE_AS_EITHER) VALUES ('2003-12-24', '2003-12-24', '2003-12-24');
ASSERT VALUES `OUTPUT` (parse_as_date, parse_as_year, parse_as_either) VALUES ('2008-08-13', '2008-08-13', '2008-08-13');
ASSERT VALUES `OUTPUT` (parse_as_date, parse_as_year, parse_as_either) VALUES (NULL, '2004-01-01', '2004-01-01');
ASSERT VALUES `OUTPUT` (parse_as_date, parse_as_year, parse_as_either) VALUES (NULL, '2012-01-01', '2012-01-01');

--@test: null - if null
CREATE STREAM INPUT (COL0 INT KEY, COL1 STRING, COL2 ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT COL0, IFNULL(COL0, 10) AS A, IFNULL(COL1, 'x') AS B, IFNULL(COL2, ARRAY[10, 20]) AS C FROM INPUT;
INSERT INTO `INPUT` (COL0, COL1, COL2) VALUES (1, 'not null', ARRAY[1, 2, 3]);
INSERT INTO `INPUT` (COl0) VALUES (NULL);
INSERT INTO `INPUT` (COl0) VALUES (NULL);
INSERT INTO `INPUT` (COL0, COL1, COL2) VALUES (2, 'not null', ARRAY[4, 5, 6]);
ASSERT VALUES `OUTPUT` (COL0, A, B, C) VALUES (1, 1, 'not null', ARRAY[1, 2, 3]);
ASSERT VALUES `OUTPUT` (A, B, C) VALUES (10, 'x', ARRAY[10, 20]);
ASSERT VALUES `OUTPUT` (COL0) VALUES (NULL);
ASSERT VALUES `OUTPUT` (COL0, A, B, C) VALUES (2, 2, 'not null', ARRAY[4, 5, 6]);

--@test: null - if null with nested structs
CREATE STREAM INPUT (COL0 INT KEY, COL1 STRUCT<Person STRUCT<Name STRING>>, COL2 ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT COL0, IFNULL(COL0, 10) AS A, IFNULL(COL1->Person->Name, 'x') AS B, IFNULL(COL2, ARRAY[10, 20]) AS C FROM INPUT;
INSERT INTO `INPUT` (COL0, COL1, COL2) VALUES (1, STRUCT(Person:=STRUCT(Name:='George')), ARRAY[1, 2, 3]);
INSERT INTO `INPUT` (COl0) VALUES (NULL);
INSERT INTO `INPUT` (COl0) VALUES (NULL);
INSERT INTO `INPUT` (COL0, COL1, COL2) VALUES (2, STRUCT(Person:=NULL), ARRAY[4, 5, 6]);
ASSERT VALUES `OUTPUT` (COL0, A, B, C) VALUES (1, 1, 'George', ARRAY[1, 2, 3]);
ASSERT VALUES `OUTPUT` (A, B, C) VALUES (10, 'x', ARRAY[10, 20]);
ASSERT VALUES `OUTPUT` (COL0) VALUES (NULL);
ASSERT VALUES `OUTPUT` (COL0, A, B, C) VALUES (2, 2, 'x', ARRAY[4, 5, 6]);

--@test: null - dereference nested structs with nulls
CREATE STREAM INPUT (COL0 INT KEY, COL1 STRUCT<Person STRUCT<Name STRING>>, COL2 ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT COL0, IFNULL(COL0, 10) AS A, COL1->Person->Name AS B, IFNULL(COL2, ARRAY[10, 20]) AS C FROM INPUT;
INSERT INTO `INPUT` (COL0, COL1, COL2) VALUES (1, STRUCT(Person:=STRUCT(Name:='George')), ARRAY[1, 2, 3]);
INSERT INTO `INPUT` (COl0) VALUES (NULL);
INSERT INTO `INPUT` (COl0) VALUES (NULL);
INSERT INTO `INPUT` (COL0, COL1, COL2) VALUES (2, STRUCT(Person:=NULL), ARRAY[4, 5, 6]);
ASSERT VALUES `OUTPUT` (COL0, A, B, C) VALUES (1, 1, 'George', ARRAY[1, 2, 3]);
ASSERT VALUES `OUTPUT` (A, B, C) VALUES (10, NULL, ARRAY[10, 20]);
ASSERT VALUES `OUTPUT` (COL0) VALUES (NULL);
ASSERT VALUES `OUTPUT` (COL0, A, B, C) VALUES (2, 2, NULL, ARRAY[4, 5, 6]);

--@test: null - log no errors when there is a null map
CREATE STREAM INPUT (ID STRING KEY, A_MAP MAP<STRING, INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, A_MAP['expected'], A_MAP['missing'], IFNULL(A_MAP['expected'], -1), IFNULL(A_MAP['missing'], -1)  FROM INPUT;
INSERT INTO `INPUT` (A_MAP) VALUES (MAP('expected':=10));
INSERT INTO `INPUT` (A_MAP) VALUES (NULL);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES (10, NULL, 10, -1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES (NULL, NULL, -1, -1);

--@test: null - log no errors when there is a null array
CREATE STREAM INPUT (ID STRING KEY, col0 ARRAY<ARRAY<INT>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, col0[1][2], IFNULL(col0[1][2], -1) FROM INPUT;
INSERT INTO `INPUT` (col0) VALUES (ARRAY[ARRAY[0, 1], ARRAY[2]]);
INSERT INTO `INPUT` (col0) VALUES (ARRAY[NULL, ARRAY[2]]);
INSERT INTO `INPUT` (col0) VALUES (NULL);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (NULL, -1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (NULL, -1);

--@test: null - NULL column
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: mismatched input 'NULL'
CREATE STREAM INPUT (ROWKEY INT KEY, COL0 NULL) WITH (kafka_topic='test_topic', value_format='JSON');
--@test: null - NULL element type
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: mismatched input 'NULL'
CREATE STREAM INPUT (ID INT KEY, COL0 ARRAY<NULL>) WITH (kafka_topic='test_topic', value_format='JSON');
--@test: null - NULL value type
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: mismatched input 'NULL'
CREATE STREAM INPUT (ID INT KEY, COL0 MAP<STRING, NULL>) WITH (kafka_topic='test_topic', value_format='JSON');
--@test: null - NULL field type
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: mismatched input 'NULL'
CREATE STREAM INPUT (ID INT KEY, COL0 STRUCT<fo NULL>) WITH (kafka_topic='test_topic', value_format='JSON');
--@test: null - null join expression
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid comparison expression 'null' in join '(L.A = null)'. Each side of the join comparision must contain references from exactly one source.
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM L JOIN R WITHIN 10 SECONDS ON L.A = null;
--@test: null - null as column
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Can't infer a type of null. Please explicitly cast it to a required type, e.g. CAST(null AS VARCHAR).
CREATE STREAM INPUT (COL0 INT KEY) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT COL0, null as COL2 FROM INPUT;
--@test: null - null if
CREATE STREAM INPUT (COL0 INT KEY, COL1 STRING, COL2 ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT COL0, NULLIF(COL0, 10) AS A, NULLIF(COL1, 'x') AS B, NULLIF(COL2, ARRAY[1, 2, 3]) AS C FROM INPUT;
INSERT INTO `INPUT` (COL0, COL1, COL2) VALUES (1, 'not null', ARRAY[1, 2, 3]);
INSERT INTO `INPUT` (COl0) VALUES (NULL);
INSERT INTO `INPUT` (COl0) VALUES (NULL);
INSERT INTO `INPUT` (COL0, COL1, COL2) VALUES (2, 'not null', ARRAY[4, 5, 6]);
INSERT INTO `INPUT` (COL0, COL1, COL2) VALUES (3, 'not null', ARRAY[]);
INSERT INTO `INPUT` (COL0, COL1, COL2) VALUES (4, NULL, ARRAY[7, 8]);
INSERT INTO `INPUT` (COL0, COL1, COL2) VALUES (10, 'x', ARRAY[7, 8]);
INSERT INTO `INPUT` (COL0, COL1, COL2) VALUES (1, 'X', ARRAY[7, 8]);
INSERT INTO `INPUT` (COL1, COL2) VALUES ('x', ARRAY[7, 8]);
INSERT INTO `INPUT` (COL1, COL2) VALUES (NULL, ARRAY[7, 8]);
ASSERT VALUES `OUTPUT` (COL0, A, B, C) VALUES (1, 1, 'not null', NULL);
ASSERT VALUES `OUTPUT` (A, B, C) VALUES (NULL, NULL, NULL);
ASSERT VALUES `OUTPUT` (COL0) VALUES (NULL);
ASSERT VALUES `OUTPUT` (COL0, A, B, C) VALUES (2, 2, 'not null', ARRAY[4, 5, 6]);
ASSERT VALUES `OUTPUT` (COL0, A, B, C) VALUES (3, 3, 'not null', ARRAY[]);
ASSERT VALUES `OUTPUT` (COL0, A, B, C) VALUES (4, 4, NULL, ARRAY[7, 8]);
ASSERT VALUES `OUTPUT` (COL0, A, B, C) VALUES (10, NULL, NULL, ARRAY[7, 8]);
ASSERT VALUES `OUTPUT` (COL0, A, B, C) VALUES (1, 1, 'X', ARRAY[7, 8]);
ASSERT VALUES `OUTPUT` (A, B, C) VALUES (NULL, NULL, ARRAY[7, 8]);
ASSERT VALUES `OUTPUT` (A, B, C) VALUES (NULL, NULL, ARRAY[7, 8]);

