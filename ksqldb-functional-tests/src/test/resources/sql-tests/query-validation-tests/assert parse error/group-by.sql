--@test: group-by - udafs only in having (stream->table)
CREATE STREAM INPUT (NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT NAME, LEN(NAME) AS LEN FROM INPUT GROUP BY NAME HAVING COUNT(NAME) = 2;
INSERT INTO `INPUT` (NAME, ROWTIME) VALUES ('bob', 1);
INSERT INTO `INPUT` (NAME, ROWTIME) VALUES ('bob', 2);
INSERT INTO `INPUT` (NAME, ROWTIME) VALUES ('bob', 3);
ASSERT VALUES `OUTPUT` (NAME, LEN, ROWTIME) VALUES ('bob', 3, 2);
ASSERT VALUES `OUTPUT` (NAME) VALUES ('bob');
ASSERT table OUTPUT (NAME STRING PRIMARY KEY, LEN INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - all columns - repartition (stream->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The projection contains no value columns.
CREATE STREAM INPUT (NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT NAME FROM INPUT GROUP BY NAME HAVING COUNT(NAME) = 1;
--@test: group-by - all columns - no repartition (stream->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The projection contains no value columns.
CREATE STREAM INPUT (NAME STRING KEY, V0 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT NAME FROM INPUT GROUP BY NAME HAVING COUNT(NAME) = 1;
--@test: group-by - value column (stream->table)
CREATE STREAM TEST (ID INT KEY, data STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT DATA, COUNT(*) AS COUNT FROM TEST GROUP BY DATA;
INSERT INTO `TEST` (ID, data) VALUES (0, 'd1');
INSERT INTO `TEST` (ID, data) VALUES (1, 'd2');
INSERT INTO `TEST` (ID, data) VALUES (2, 'd1');
INSERT INTO `TEST` (ID, data) VALUES (3, 'd2');
INSERT INTO `TEST` (ID, data) VALUES (4, 'd1');
ASSERT VALUES `OUTPUT` (DATA, COUNT) VALUES ('d1', 1);
ASSERT VALUES `OUTPUT` (DATA, COUNT) VALUES ('d2', 1);
ASSERT VALUES `OUTPUT` (DATA, COUNT) VALUES ('d1', 2);
ASSERT VALUES `OUTPUT` (DATA, COUNT) VALUES ('d2', 2);
ASSERT VALUES `OUTPUT` (DATA, COUNT) VALUES ('d1', 3);
ASSERT table OUTPUT (DATA STRING PRIMARY KEY, COUNT BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - struct field (stream->table)
CREATE STREAM TEST (ID INT KEY, ADDRESS STRUCT<STREET STRING, TOWN STRING>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT ADDRESS->TOWN, COUNT(*) AS COUNT FROM TEST GROUP BY ADDRESS->TOWN;
INSERT INTO `TEST` (ID, ADDRESS) VALUES (0, STRUCT(STREET:='1st Steet', Town:='Oxford'));
INSERT INTO `TEST` (ID, ADDRESS) VALUES (1, STRUCT(STREET:='1st Steet', Town:='London'));
INSERT INTO `TEST` (ID, ADDRESS) VALUES (2, STRUCT(STREET:='1st Steet', Town:='Oxford'));
INSERT INTO `TEST` (ID, ADDRESS) VALUES (3, STRUCT(STREET:='1st Steet', Town:='London'));
INSERT INTO `TEST` (ID, ADDRESS) VALUES (4, STRUCT(STREET:='1st Steet', Town:='Oxford'));
ASSERT VALUES `OUTPUT` (TOWN, COUNT) VALUES ('Oxford', 1);
ASSERT VALUES `OUTPUT` (TOWN, COUNT) VALUES ('London', 1);
ASSERT VALUES `OUTPUT` (TOWN, COUNT) VALUES ('Oxford', 2);
ASSERT VALUES `OUTPUT` (TOWN, COUNT) VALUES ('London', 2);
ASSERT VALUES `OUTPUT` (TOWN, COUNT) VALUES ('Oxford', 3);
ASSERT table OUTPUT (TOWN STRING PRIMARY KEY, COUNT BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - single expression (stream->table)
CREATE STREAM TEST (ID INT KEY, DATA STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT LEN(DATA), COUNT(*) AS COUNT FROM TEST GROUP BY LEN(DATA);
INSERT INTO `TEST` (ID, data) VALUES (0, '22');
INSERT INTO `TEST` (ID, data) VALUES (1, '333');
INSERT INTO `TEST` (ID, data) VALUES (2, '-2');
INSERT INTO `TEST` (ID, data) VALUES (3, '003');
INSERT INTO `TEST` (ID, data) VALUES (4, '2-');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, COUNT) VALUES (2, 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, COUNT) VALUES (3, 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, COUNT) VALUES (2, 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, COUNT) VALUES (3, 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, COUNT) VALUES (2, 3);
ASSERT table OUTPUT (KSQL_COL_0 INT PRIMARY KEY, COUNT BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - single column with alias (stream->table)
CREATE STREAM TEST (ID INT KEY, data STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT DATA AS NEW_KEY, COUNT(*) AS COUNT FROM TEST GROUP BY DATA;
INSERT INTO `TEST` (ID, data) VALUES (0, 'd1');
INSERT INTO `TEST` (ID, data) VALUES (1, 'd2');
INSERT INTO `TEST` (ID, data) VALUES (2, 'd1');
INSERT INTO `TEST` (ID, data) VALUES (3, 'd2');
INSERT INTO `TEST` (ID, data) VALUES (4, 'd1');
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES ('d1', 1);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES ('d2', 1);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES ('d1', 2);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES ('d2', 2);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES ('d1', 3);
ASSERT table OUTPUT (NEW_KEY STRING PRIMARY KEY, COUNT BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - single column with alias (table->table)
CREATE TABLE TEST (ID INT PRIMARY KEY, data STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT DATA AS NEW_KEY, COUNT(*) AS COUNT FROM TEST GROUP BY DATA;
INSERT INTO `TEST` (ID, data) VALUES (0, 'd1');
INSERT INTO `TEST` (ID, data) VALUES (1, 'd2');
INSERT INTO `TEST` (ID, data) VALUES (2, 'd1');
INSERT INTO `TEST` (ID, data) VALUES (3, 'd2');
INSERT INTO `TEST` (ID, data) VALUES (4, 'd1');
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES ('d1', 1);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES ('d2', 1);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES ('d1', 2);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES ('d2', 2);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES ('d1', 3);
ASSERT table OUTPUT (NEW_KEY STRING PRIMARY KEY, COUNT BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - single expression with alias (stream->table)
CREATE STREAM TEST (ID INT KEY, data STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT LEN(DATA) AS NEW_KEY, COUNT(*) AS COUNT FROM TEST GROUP BY LEN(DATA);
INSERT INTO `TEST` (ID, data) VALUES (0, '22');
INSERT INTO `TEST` (ID, data) VALUES (1, '333');
INSERT INTO `TEST` (ID, data) VALUES (2, '-2');
INSERT INTO `TEST` (ID, data) VALUES (3, '003');
INSERT INTO `TEST` (ID, data) VALUES (4, '2-');
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES (2, 1);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES (3, 1);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES (2, 2);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES (3, 2);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES (2, 3);
ASSERT table OUTPUT (NEW_KEY INT PRIMARY KEY, COUNT BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - single expression with alias (table->table)
CREATE TABLE TEST (ID INT PRIMARY KEY, data STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT LEN(DATA) AS NEW_KEY, COUNT(*) AS COUNT FROM TEST GROUP BY LEN(DATA);
INSERT INTO `TEST` (ID, data) VALUES (0, '22');
INSERT INTO `TEST` (ID, data) VALUES (1, '333');
INSERT INTO `TEST` (ID, data) VALUES (2, '-2');
INSERT INTO `TEST` (ID, data) VALUES (3, '003');
INSERT INTO `TEST` (ID, data) VALUES (4, '2-');
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES (2, 1);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES (3, 1);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES (2, 2);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES (3, 2);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES (2, 3);
ASSERT table OUTPUT (NEW_KEY INT PRIMARY KEY, COUNT BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - steam with no key
CREATE STREAM TEST (data INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT DATA, COUNT(*) AS COUNT FROM TEST GROUP BY DATA;
INSERT INTO `TEST` (data) VALUES (22);
INSERT INTO `TEST` (data) VALUES (333);
INSERT INTO `TEST` (data) VALUES (22);
ASSERT VALUES `OUTPUT` (DATA, COUNT) VALUES (22, 1);
ASSERT VALUES `OUTPUT` (DATA, COUNT) VALUES (333, 1);
ASSERT VALUES `OUTPUT` (DATA, COUNT) VALUES (22, 2);
ASSERT table OUTPUT (DATA INT PRIMARY KEY, COUNT BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - subscript in group-by and select
CREATE STREAM INPUT (id INT KEY, col1 MAP<VARCHAR, VARCHAR>) WITH (kafka_topic='test_topic', value_format='json');
CREATE TABLE OUTPUT AS SELECT col1['foo'] AS NEW_KEY, COUNT(*) AS COUNT FROM INPUT GROUP BY col1['foo'];
INSERT INTO `INPUT` (ID, col1) VALUES (0, MAP('foo':='lala'));
INSERT INTO `INPUT` (ID, col1) VALUES (1, MAP('foo':='kaka'));
INSERT INTO `INPUT` (ID, col1) VALUES (2, MAP('alice':='wonderland'));
INSERT INTO `INPUT` (ID, col1) VALUES (3, MAP('mary':='lamb'));
INSERT INTO `INPUT` (ID, col1) VALUES (4, NULL);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES ('lala', 1);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES ('kaka', 1);

--@test: group-by - subscript in group-by and having
CREATE STREAM INPUT (id INT KEY, col1 MAP<VARCHAR, VARCHAR>) WITH (kafka_topic='test_topic', value_format='json');
CREATE TABLE OUTPUT AS SELECT col1['foo'] AS NEW_KEY, COUNT(*) AS COUNT FROM INPUT GROUP BY col1['foo'] HAVING col1['foo']='lala';
INSERT INTO `INPUT` (ID, col1) VALUES (0, MAP('foo':='lala'));
INSERT INTO `INPUT` (ID, col1) VALUES (1, MAP('foo':='kaka'));
INSERT INTO `INPUT` (ID, col1) VALUES (2, MAP('alice':='wonderland'));
INSERT INTO `INPUT` (ID, col1) VALUES (3, MAP('mary':='lamb'));
INSERT INTO `INPUT` (ID, col1) VALUES (4, NULL);
ASSERT VALUES `OUTPUT` (NEW_KEY, COUNT) VALUES ('lala', 1);

--@test: group-by - subscript in group-by and non aggregate function in select
CREATE STREAM INPUT (id INT KEY, col1 MAP<VARCHAR, VARCHAR>) WITH (kafka_topic='test_topic', value_format='json');
CREATE TABLE OUTPUT AS SELECT col1['foo'] AS NEW_KEY, AS_VALUE(col1['foo']) as VV, COUNT(*) AS COUNT FROM INPUT GROUP BY col1['foo'];
INSERT INTO `INPUT` (ID, col1) VALUES (0, MAP('foo':='lala'));
INSERT INTO `INPUT` (ID, col1) VALUES (1, MAP('foo':='kaka'));
INSERT INTO `INPUT` (ID, col1) VALUES (2, MAP('alice':='wonderland'));
INSERT INTO `INPUT` (ID, col1) VALUES (3, MAP('mary':='lamb'));
INSERT INTO `INPUT` (ID, col1) VALUES (4, NULL);
ASSERT VALUES `OUTPUT` (NEW_KEY, VV, COUNT) VALUES ('lala', 'lala', 1);
ASSERT VALUES `OUTPUT` (NEW_KEY, VV, COUNT) VALUES ('kaka', 'kaka', 1);

--@test: group-by - struct in group-by and non aggregate function in select
CREATE STREAM INPUT (id INT KEY, col1 STRUCT<a VARCHAR, b INT>) WITH (kafka_topic='test_topic', value_format='json');
CREATE TABLE OUTPUT AS SELECT col1->a AS NEW_KEY, AS_VALUE(col1->a) as VV, COUNT(*) AS COUNT FROM INPUT GROUP BY col1->a;
INSERT INTO `INPUT` (ID, col1) VALUES (0, STRUCT(a:='lala', b:=1));
INSERT INTO `INPUT` (ID, col1) VALUES (1, STRUCT(a:='lala', b:=2));
INSERT INTO `INPUT` (ID, col1) VALUES (2, STRUCT(a:='wonderland', b:=3));
INSERT INTO `INPUT` (ID, col1) VALUES (3, STRUCT(a:='lamb', b:=4));
INSERT INTO `INPUT` (ID, col1) VALUES (4, NULL);
ASSERT VALUES `OUTPUT` (NEW_KEY, VV, COUNT) VALUES ('lala', 'lala', 1);
ASSERT VALUES `OUTPUT` (NEW_KEY, VV, COUNT) VALUES ('lala', 'lala', 2);
ASSERT VALUES `OUTPUT` (NEW_KEY, VV, COUNT) VALUES ('wonderland', 'wonderland', 1);
ASSERT VALUES `OUTPUT` (NEW_KEY, VV, COUNT) VALUES ('lamb', 'lamb', 1);

--@test: group-by - function in group-by and nested function in select
CREATE STREAM INPUT (id INT KEY, col1 VARCHAR, col2 VARCHAR, col3 VARCHAR) WITH (kafka_topic='test_topic', format='json');
CREATE TABLE OUTPUT AS SELECT INITCAP(COL1) AS G1, COL2 AS G2, TRIM(COL3) AS G3, concat(initcap(col1), col2, trim(col3)) AS foo, COUNT(*) FROM input GROUP BY INITCAP(col1), col2, TRIM(col3);
INSERT INTO `INPUT` (ID, col1, col2, col3) VALUES (0, 'smells', 'like', 'teen spirit');
INSERT INTO `INPUT` (ID, col1, col2, col3) VALUES (1, 'the', 'man who', 'stole the world');
INSERT INTO `INPUT` (ID, col1, col2, col3) VALUES (2, 'smells', 'like', 'spring');
INSERT INTO `INPUT` (ID, col1, col2, col3) VALUES (3, 'smells', 'like', '   teen spirit   ');
ASSERT VALUES `OUTPUT` (G1, G2, G3, FOO, KSQL_COL_0) VALUES ('Smells', 'like', 'teen spirit', 'Smellsliketeen spirit', 1);
ASSERT VALUES `OUTPUT` (G1, G2, G3, FOO, KSQL_COL_0) VALUES ('The', 'man who', 'stole the world', 'Theman whostole the world', 1);
ASSERT VALUES `OUTPUT` (G1, G2, G3, FOO, KSQL_COL_0) VALUES ('Smells', 'like', 'spring', 'Smellslikespring', 1);
ASSERT VALUES `OUTPUT` (G1, G2, G3, FOO, KSQL_COL_0) VALUES ('Smells', 'like', 'teen spirit', 'Smellsliketeen spirit', 2);

--@test: group-by - group by column in nested non-aggregate function in select
CREATE STREAM INPUT (id INT KEY, col1 VARCHAR, col2 VARCHAR, col3 VARCHAR) WITH (kafka_topic='test_topic', format='json');
CREATE TABLE OUTPUT AS SELECT INITCAP(COL1) AS G1, COL2 AS G2, COL3 AS G3, concat(initcap(col1), col2, trim(col3)) AS foo, COUNT(*) FROM input GROUP BY INITCAP(col1), col2, col3;
INSERT INTO `INPUT` (ID, col1, col2, col3) VALUES (0, 'smells', 'like', 'teen spirit');
INSERT INTO `INPUT` (ID, col1, col2, col3) VALUES (1, 'the', 'man who', 'stole the world');
INSERT INTO `INPUT` (ID, col1, col2, col3) VALUES (2, 'smells', 'like', 'spring');
INSERT INTO `INPUT` (ID, col1, col2, col3) VALUES (3, 'smells', 'like', '   teen spirit   ');
ASSERT VALUES `OUTPUT` (G1, G2, G3, FOO, KSQL_COL_0) VALUES ('Smells', 'like', 'teen spirit', 'Smellsliketeen spirit', 1);
ASSERT VALUES `OUTPUT` (G1, G2, G3, FOO, KSQL_COL_0) VALUES ('The', 'man who', 'stole the world', 'Theman whostole the world', 1);
ASSERT VALUES `OUTPUT` (G1, G2, G3, FOO, KSQL_COL_0) VALUES ('Smells', 'like', 'spring', 'Smellslikespring', 1);
ASSERT VALUES `OUTPUT` (G1, G2, G3, FOO, KSQL_COL_0) VALUES ('Smells', 'like', '   teen spirit   ', 'Smellsliketeen spirit', 1);

--@test: group-by - function group by column used in non-aggregate function in having
CREATE STREAM INPUT (id INT KEY, col1 VARCHAR, col2 VARCHAR, col3 VARCHAR) WITH (kafka_topic='test_topic', format='json');
CREATE TABLE OUTPUT AS SELECT INITCAP(COL1) AS G1, COL2 AS G2, trim(COL3) AS G3, COUNT(*) FROM input GROUP BY INITCAP(col1), col2, trim(col3) HAVING substring(trim(col3),1,4) = 'teen';
INSERT INTO `INPUT` (ID, col1, col2, col3) VALUES (0, 'smells', 'like', 'teen spirit');
INSERT INTO `INPUT` (ID, col1, col2, col3) VALUES (1, 'the', 'man who', 'stole the world');
INSERT INTO `INPUT` (ID, col1, col2, col3) VALUES (2, 'smells', 'like', 'spring');
INSERT INTO `INPUT` (ID, col1, col2, col3) VALUES (3, 'smells', 'like', '   teen spirit   ');
ASSERT VALUES `OUTPUT` (G1, G2, G3, KSQL_COL_0) VALUES ('Smells', 'like', 'teen spirit', 1);
ASSERT VALUES `OUTPUT` (G1, G2, G3, KSQL_COL_0) VALUES ('Smells', 'like', 'teen spirit', 2);

--@test: group-by - arithmetic in group by column used in non-aggregate function in select
CREATE STREAM INPUT (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='test_topic', value_format='json');
CREATE TABLE OUTPUT AS SELECT col1+col2 AS G1, AS_VALUE(col1+col2), COUNT(*) FROM input GROUP BY col1+col2;
INSERT INTO `INPUT` (ID, col1, col2) VALUES (0, 1, 1);
INSERT INTO `INPUT` (ID, col1, col2) VALUES (1, 2, 2);
INSERT INTO `INPUT` (ID, col1, col2) VALUES (2, 3, 3);
INSERT INTO `INPUT` (ID, col1, col2) VALUES (3, 4, 4);
ASSERT VALUES `OUTPUT` (G1, KSQL_COL_0, KSQL_COL_1) VALUES (2, 2, 1);
ASSERT VALUES `OUTPUT` (G1, KSQL_COL_0, KSQL_COL_1) VALUES (4, 4, 1);
ASSERT VALUES `OUTPUT` (G1, KSQL_COL_0, KSQL_COL_1) VALUES (6, 6, 1);
ASSERT VALUES `OUTPUT` (G1, KSQL_COL_0, KSQL_COL_1) VALUES (8, 8, 1);

--@test: group-by - expressions used in non-aggregate function in select whose children are not part of group-by
CREATE STREAM INPUT (id INT KEY, col1 MAP<VARCHAR, INT>, col2 MAP<VARCHAR, INT>) WITH (kafka_topic='test_topic', value_format='json');
CREATE TABLE OUTPUT AS SELECT col1['foo']+col2['bar'] AS G1, AS_VALUE(col1['foo']+col2['bar']), COUNT(*) FROM input GROUP BY col1['foo']+col2['bar'];
INSERT INTO `INPUT` (ID, col1, col2) VALUES (0, MAP('a':=1), MAP('b':=1));
INSERT INTO `INPUT` (ID, col1, col2) VALUES (1, MAP('foo':=1), MAP('bar':=1));
INSERT INTO `INPUT` (ID, col1, col2) VALUES (2, MAP('bar':=1), MAP('foo':=1));
INSERT INTO `INPUT` (ID, col1, col2) VALUES (3, MAP('foo':=1), MAP('foo':=1));
ASSERT VALUES `OUTPUT` (G1, KSQL_COL_0, KSQL_COL_1) VALUES (2, 2, 1);

--@test: group-by - unknown function
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Can't find any functions with the name 'WONT_FIND_ME'
CREATE STREAM TEST (ID INT KEY, data STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT DATA, WONT_FIND_ME(ID) FROM TEST GROUP BY DATA;
--@test: group-by - unknown function - multi group by
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Can't find any functions with the name 'WONT_FIND_ME'
CREATE STREAM TEST (ID INT KEY, data STRING, other STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT DATA, OTHER, WONT_FIND_ME(ID) FROM TEST GROUP BY DATA, OTHER;
--@test: group-by - non-table udaf on table
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The aggregation functions LATEST_BY_OFFSET, MIN and MAX cannot be applied to a table source, only to a stream source.
CREATE TABLE INPUT (ID BIGINT PRIMARY KEY, F0 INT, F1 BIGINT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT ID, LATEST_BY_OFFSET(F0), MIN(F1), MAX(F1) FROM INPUT GROUP BY ID;
--@test: group-by - multiple expressions
CREATE STREAM TEST (f1 INT KEY, f2 INT) WITH (kafka_topic='test_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT f2, f1, f2+f1, COUNT(*) FROM TEST GROUP BY f1, f2;
INSERT INTO `TEST` (F1, f2) VALUES (1, 2);
INSERT INTO `TEST` (F1, f2) VALUES (2, 4);
INSERT INTO `TEST` (F1, f2) VALUES (1, NULL);
INSERT INTO `TEST` (F1) VALUES (1);
INSERT INTO `TEST` (f2) VALUES (1);
INSERT INTO `TEST` (F1, f2) VALUES (1, 2);
INSERT INTO `TEST` (F1, f2) VALUES (2, 4);
INSERT INTO `TEST` (F1, f2) VALUES (2, 1);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1) VALUES (1, 2, 3, 1);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1) VALUES (2, 4, 6, 1);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1) VALUES (1, 2, 3, 2);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1) VALUES (2, 4, 6, 2);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1) VALUES (2, 1, 3, 1);
ASSERT table OUTPUT (F1 INT PRIMARY KEY, F2 INT PRIMARY KEY, KSQL_COL_0 INT, KSQL_COL_1 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - multiple expressions with struct field and other expression
CREATE STREAM TEST (f1 INT KEY, f2 INT, address STRUCT<street STRING, town STRING>) WITH (kafka_topic='test_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT address->town, f1, 2*f2+f1, COUNT(*), 2*f2 FROM TEST GROUP BY f1, address->town, 2*f2;
INSERT INTO `TEST` (F1, f2, ADDRESS) VALUES (1, 2, STRUCT(STREET:='1st Street', Town:='Oxford'));
INSERT INTO `TEST` (F1, f2, ADDRESS) VALUES (2, 4, STRUCT(STREET:='1st Street', Town:='London'));
INSERT INTO `TEST` (F1, f2, ADDRESS) VALUES (1, 2, STRUCT(STREET:='1st Street', Town:='Oxford'));
INSERT INTO `TEST` (F1, f2, ADDRESS) VALUES (2, 4, STRUCT(STREET:='1st Street', Town:='London'));
INSERT INTO `TEST` (F1, f2, ADDRESS) VALUES (2, 1, STRUCT(STREET:='1st Street', Town:='Oxford'));
ASSERT VALUES `OUTPUT` (F1, TOWN, KSQL_COL_2, KSQL_COL_0, KSQL_COL_1) VALUES (1, 'Oxford', 4, 5, 1);
ASSERT VALUES `OUTPUT` (F1, TOWN, KSQL_COL_2, KSQL_COL_0, KSQL_COL_1) VALUES (2, 'London', 8, 10, 1);
ASSERT VALUES `OUTPUT` (F1, TOWN, KSQL_COL_2, KSQL_COL_0, KSQL_COL_1) VALUES (1, 'Oxford', 4, 5, 2);
ASSERT VALUES `OUTPUT` (F1, TOWN, KSQL_COL_2, KSQL_COL_0, KSQL_COL_1) VALUES (2, 'London', 8, 10, 2);
ASSERT VALUES `OUTPUT` (F1, TOWN, KSQL_COL_2, KSQL_COL_0, KSQL_COL_1) VALUES (2, 'Oxford', 2, 4, 1);
ASSERT table OUTPUT (F1 INT PRIMARY KEY, TOWN STRING PRIMARY KEY, KSQL_COL_2 INT PRIMARY KEY, KSQL_COL_0 INT, KSQL_COL_1 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - multiple expressions - KAFKA key format
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Key format does not support schema.
CREATE STREAM TEST (f1 INT KEY, f2 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT f2, f1, f2+f1, COUNT(*) FROM TEST GROUP BY f1, f2;

--@test: group-by - multiple expressions - KAFKA to non-KAFKA key format
CREATE STREAM TEST (f1 INT KEY, f2 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT WITH (KEY_FORMAT='JSON') AS SELECT f2, f1, f2+f1, COUNT(*) FROM TEST GROUP BY f1, f2;
INSERT INTO `TEST` (F1, f2) VALUES (1, 2);
INSERT INTO `TEST` (F1, f2) VALUES (2, 4);
INSERT INTO `TEST` (F1, f2) VALUES (1, NULL);
INSERT INTO `TEST` (F1) VALUES (1);
INSERT INTO `TEST` (f2) VALUES (1);
INSERT INTO `TEST` (F1, f2) VALUES (1, 2);
INSERT INTO `TEST` (F1, f2) VALUES (2, 4);
INSERT INTO `TEST` (F1, f2) VALUES (2, 1);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1) VALUES (1, 2, 3, 1);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1) VALUES (2, 4, 6, 1);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1) VALUES (1, 2, 3, 2);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1) VALUES (2, 4, 6, 2);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1) VALUES (2, 1, 3, 1);
ASSERT table OUTPUT (F1 INT PRIMARY KEY, F2 INT PRIMARY KEY, KSQL_COL_0 INT, KSQL_COL_1 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - multiple expressions - KAFKA to struct key format
CREATE STREAM TEST (f1 INT KEY, f2 INT) WITH (kafka_topic='test_topic', value_format='JSON', partitions=1);
CREATE TABLE OUTPUT WITH (KEY_FORMAT='JSON') AS SELECT STRUCT(f1:=f1, f2:=f2) AS k, as_value(struct(f1:=f1, f2:=f2)) AS key, COUNT(*) AS total FROM TEST GROUP BY STRUCT(f1:=f1, f2:=f2);
INSERT INTO `TEST` (F1, f2) VALUES (1, 2);
INSERT INTO `TEST` (F1, f2) VALUES (2, 4);
INSERT INTO `TEST` (F1, f2) VALUES (1, NULL);
INSERT INTO `TEST` (F1) VALUES (1);
INSERT INTO `TEST` (f2) VALUES (1);
INSERT INTO `TEST` (F1, f2) VALUES (1, 2);
INSERT INTO `TEST` (F1, f2) VALUES (2, 4);
INSERT INTO `TEST` (F1, f2) VALUES (2, 1);
ASSERT VALUES `OUTPUT` (K, KEY, TOTAL) VALUES (STRUCT(F1:=1, F2:=2), STRUCT(F1:=1, F2:=2), 1);
ASSERT VALUES `OUTPUT` (K, KEY, TOTAL) VALUES (STRUCT(F1:=2, F2:=4), STRUCT(F1:=2, F2:=4), 1);
ASSERT VALUES `OUTPUT` (K, KEY, TOTAL) VALUES (STRUCT(F1:=1, F2:=NULL), STRUCT(F1:=1, F2:=NULL), 1);
ASSERT VALUES `OUTPUT` (K, KEY, TOTAL) VALUES (STRUCT(F1:=NULL, F2:=1), STRUCT(F1:=NULL, F2:=1), 1);
ASSERT VALUES `OUTPUT` (K, KEY, TOTAL) VALUES (STRUCT(F1:=1, F2:=2), STRUCT(F1:=1, F2:=2), 2);
ASSERT VALUES `OUTPUT` (K, KEY, TOTAL) VALUES (STRUCT(F1:=2, F2:=4), STRUCT(F1:=2, F2:=4), 2);
ASSERT VALUES `OUTPUT` (K, KEY, TOTAL) VALUES (STRUCT(F1:=2, F2:=1), STRUCT(F1:=2, F2:=1), 1);

--@test: group-by - multiple expressions - KAFKA to non-supported decimal key format
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Key format does not support schema.
CREATE STREAM TEST (f1 INT KEY, f2 DECIMAL(2, 1)) WITH (kafka_topic='test_topic', value_format='JSON', partitions=1);
CREATE TABLE OUTPUT AS SELECT f2 AS key, COUNT(*) AS total FROM TEST GROUP BY f2;

--@test: group-by - multiple expressions - KAFKA to non-supported decimal key format with JSON format conversion
CREATE STREAM TEST (f1 INT KEY, f2 DECIMAL(2, 1)) WITH (kafka_topic='test_topic', value_format='JSON', partitions=1);
CREATE TABLE OUTPUT WITH (KEY_FORMAT='JSON') AS SELECT f2 AS key, COUNT(*) AS total FROM TEST GROUP BY f2;
INSERT INTO `TEST` (F1, f2) VALUES (1, 2);
INSERT INTO `TEST` (F1, f2) VALUES (2, 4);
INSERT INTO `TEST` (F1, f2) VALUES (1, NULL);
INSERT INTO `TEST` (F1) VALUES (1);
INSERT INTO `TEST` (f2) VALUES (1);
INSERT INTO `TEST` (F1, f2) VALUES (1, 2);
INSERT INTO `TEST` (F1, f2) VALUES (2, 4);
INSERT INTO `TEST` (F1, f2) VALUES (2, 1);
ASSERT VALUES `OUTPUT` (KEY, TOTAL) VALUES (2.0, 1);
ASSERT VALUES `OUTPUT` (KEY, TOTAL) VALUES (4.0, 1);
ASSERT VALUES `OUTPUT` (KEY, TOTAL) VALUES (1.0, 1);
ASSERT VALUES `OUTPUT` (KEY, TOTAL) VALUES (2.0, 2);
ASSERT VALUES `OUTPUT` (KEY, TOTAL) VALUES (4.0, 2);
ASSERT VALUES `OUTPUT` (KEY, TOTAL) VALUES (1.0, 2);
ASSERT table OUTPUT (KEY DECIMAL(2, 1) PRIMARY KEY, TOTAL BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - JSON format to KAFKA key format with unsupported multiple column
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Key format does not support schema.
CREATE STREAM TEST (f1 INT KEY, f2 INT) WITH (kafka_topic='test_topic', key_format='JSON', value_format='JSON', partitions=1);
CREATE TABLE OUTPUT WITH (KEY_FORMAT='KAFKA') AS SELECT f1 AS k1, f2 AS k2, COUNT(*) AS total FROM TEST GROUP BY f1, f2;

--@test: group-by - multiple expressions - delimited key format to multi-column primitive format
CREATE STREAM TEST (f1 STRING KEY, f2 INT) WITH (kafka_topic='test_topic', key_format='DELIMITED', value_format='JSON', partitions=1);
CREATE TABLE OUTPUT AS SELECT f1 AS k1, f2 AS k2, COUNT(*) AS total FROM TEST GROUP BY f1, f2;
INSERT INTO `TEST` (F1, f2) VALUES ('a', 2);
INSERT INTO `TEST` (F1, f2) VALUES ('b', 4);
INSERT INTO `TEST` (F1, f2) VALUES ('a', NULL);
INSERT INTO `TEST` (F1) VALUES ('a');
INSERT INTO `TEST` (f2) VALUES (1);
INSERT INTO `TEST` (F1, f2) VALUES ('a', 2);
INSERT INTO `TEST` (F1, f2) VALUES ('b', 4);
INSERT INTO `TEST` (F1, f2) VALUES ('b', 1);
ASSERT VALUES `OUTPUT` (K1, K2, TOTAL) VALUES ('a', 2, 1);
ASSERT VALUES `OUTPUT` (K1, K2, TOTAL) VALUES ('b', 4, 1);
ASSERT VALUES `OUTPUT` (K1, K2, TOTAL) VALUES ('a', 2, 2);
ASSERT VALUES `OUTPUT` (K1, K2, TOTAL) VALUES ('b', 4, 2);
ASSERT VALUES `OUTPUT` (K1, K2, TOTAL) VALUES ('b', 1, 1);

--@test: group-by - multiple expressions - delimited key format to multi-column, non-primitive format
CREATE STREAM TEST (f1 STRING KEY, f2 INT) WITH (kafka_topic='test_topic', key_format='DELIMITED', value_format='JSON', partitions=1);
CREATE TABLE OUTPUT WITH (KEY_FORMAT='JSON') AS SELECT STRUCT(f1:=f1, f2:=f2) AS k1, f1 as k2, as_value(struct(f1:=f1, f2:=f2)) AS key, COUNT(*) AS total FROM TEST GROUP BY STRUCT(f1:=f1, f2:=f2), f1;
INSERT INTO `TEST` (F1, f2) VALUES ('a', 2);
INSERT INTO `TEST` (F1, f2) VALUES ('b', 4);
INSERT INTO `TEST` (F1, f2) VALUES ('a', NULL);
INSERT INTO `TEST` (F1) VALUES ('a');
INSERT INTO `TEST` (f2) VALUES (1);
INSERT INTO `TEST` (F1, f2) VALUES ('a', 2);
INSERT INTO `TEST` (F1, f2) VALUES ('b', 4);
INSERT INTO `TEST` (F1, f2) VALUES ('b', 1);
ASSERT VALUES `OUTPUT` (K1, K2, KEY, TOTAL) VALUES (STRUCT(F1:='a', F2:=2), 'a', STRUCT(F1:='a', F2:=2), 1);
ASSERT VALUES `OUTPUT` (K1, K2, KEY, TOTAL) VALUES (STRUCT(F1:='b', F2:=4), 'b', STRUCT(F1:='b', F2:=4), 1);
ASSERT VALUES `OUTPUT` (K1, K2, KEY, TOTAL) VALUES (STRUCT(F1:='a', F2:=NULL), 'a', STRUCT(F1:='a', F2:=NULL), 1);
ASSERT VALUES `OUTPUT` (K1, K2, KEY, TOTAL) VALUES (STRUCT(F1:='a', F2:=2), 'a', STRUCT(F1:='a', F2:=2), 2);
ASSERT VALUES `OUTPUT` (K1, K2, KEY, TOTAL) VALUES (STRUCT(F1:='b', F2:=4), 'b', STRUCT(F1:='b', F2:=4), 2);
ASSERT VALUES `OUTPUT` (K1, K2, KEY, TOTAL) VALUES (STRUCT(F1:='b', F2:=1), 'b', STRUCT(F1:='b', F2:=1), 1);

--@test: group-by - multiple expressions - delimited key format to struct format
CREATE STREAM TEST (f1 STRING KEY, f2 INT) WITH (kafka_topic='test_topic', key_format='DELIMITED', value_format='JSON', partitions=1);
CREATE TABLE OUTPUT WITH (KEY_FORMAT='JSON') AS SELECT STRUCT(f1:=f1, f2:=f2) AS k, as_value(struct(f1:=f1, f2:=f2)) AS key, COUNT(*) AS total FROM TEST GROUP BY STRUCT(f1:=f1, f2:=f2);
INSERT INTO `TEST` (F1, f2) VALUES ('a', 2);
INSERT INTO `TEST` (F1, f2) VALUES ('b', 4);
INSERT INTO `TEST` (F1, f2) VALUES ('a', NULL);
INSERT INTO `TEST` (F1) VALUES ('a');
INSERT INTO `TEST` (f2) VALUES (1);
INSERT INTO `TEST` (F1, f2) VALUES ('a', 2);
INSERT INTO `TEST` (F1, f2) VALUES ('b', 4);
INSERT INTO `TEST` (F1, f2) VALUES ('b', 1);
ASSERT VALUES `OUTPUT` (K, KEY, TOTAL) VALUES (STRUCT(F1:='a', F2:=2), STRUCT(F1:='a', F2:=2), 1);
ASSERT VALUES `OUTPUT` (K, KEY, TOTAL) VALUES (STRUCT(F1:='b', F2:=4), STRUCT(F1:='b', F2:=4), 1);
ASSERT VALUES `OUTPUT` (K, KEY, TOTAL) VALUES (STRUCT(F1:='a', F2:=NULL), STRUCT(F1:='a', F2:=NULL), 1);
ASSERT VALUES `OUTPUT` (K, KEY, TOTAL) VALUES (STRUCT(F1:=NULL, F2:=1), STRUCT(F1:=NULL, F2:=1), 1);
ASSERT VALUES `OUTPUT` (K, KEY, TOTAL) VALUES (STRUCT(F1:='a', F2:=2), STRUCT(F1:='a', F2:=2), 2);
ASSERT VALUES `OUTPUT` (K, KEY, TOTAL) VALUES (STRUCT(F1:='b', F2:=4), STRUCT(F1:='b', F2:=4), 2);
ASSERT VALUES `OUTPUT` (K, KEY, TOTAL) VALUES (STRUCT(F1:='b', F2:=1), STRUCT(F1:='b', F2:=1), 1);

--@test: group-by - multiple expressions - windowed
CREATE STREAM TEST (f1 INT KEY, f2 INT) WITH (kafka_topic='test_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT f2, f1, f2+f1, COUNT(*) FROM TEST WINDOW TUMBLING (SIZE 1 SECOND) GROUP BY f1, f2;
INSERT INTO `TEST` (F1, f2) VALUES (1, 2);
INSERT INTO `TEST` (F1, f2) VALUES (2, 4);
INSERT INTO `TEST` (F1, f2) VALUES (1, 2);
INSERT INTO `TEST` (F1, f2) VALUES (2, 4);
INSERT INTO `TEST` (F1, f2) VALUES (2, 1);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1, WINDOWSTART, WINDOWEND) VALUES (1, 2, 3, 1, 0, 1000);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1, WINDOWSTART, WINDOWEND) VALUES (2, 4, 6, 1, 0, 1000);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1, WINDOWSTART, WINDOWEND) VALUES (1, 2, 3, 2, 0, 1000);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1, WINDOWSTART, WINDOWEND) VALUES (2, 4, 6, 2, 0, 1000);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1, WINDOWSTART, WINDOWEND) VALUES (2, 1, 3, 1, 0, 1000);
ASSERT table OUTPUT (F1 INT PRIMARY KEY, F2 INT PRIMARY KEY, KSQL_COL_0 INT, KSQL_COL_1 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - multiple expressions - table aggregate
CREATE TABLE INPUT (ID INT PRIMARY KEY, VALUE INT) WITH (kafka_topic='test_topic', format='JSON');
CREATE TABLE OUTPUT as SELECT 1 as k, value, count(1) AS ID FROM INPUT group by value, 1;
INSERT INTO `INPUT` (ID, VALUE) VALUES (10, 0);
INSERT INTO `INPUT` (ID, VALUE) VALUES (1666, 0);
INSERT INTO `INPUT` (ID, VALUE) VALUES (98, 0);
INSERT INTO `INPUT` (ID, VALUE) VALUES (98, 1);
INSERT INTO `INPUT` (ID, VALUE) VALUES (2, NULL);
INSERT INTO `INPUT` (ID) VALUES (2);
ASSERT VALUES `OUTPUT` (VALUE, K, ID) VALUES (0, 1, 1);
ASSERT VALUES `OUTPUT` (VALUE, K, ID) VALUES (0, 1, 2);
ASSERT VALUES `OUTPUT` (VALUE, K, ID) VALUES (0, 1, 3);
ASSERT VALUES `OUTPUT` (VALUE, K, ID) VALUES (0, 1, 2);
ASSERT VALUES `OUTPUT` (VALUE, K, ID) VALUES (1, 1, 1);
ASSERT table OUTPUT (VALUE INT PRIMARY KEY, K INT PRIMARY KEY, ID BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - multiple expressions - table aggregate - KAFKA to non-KAFKA key format
CREATE TABLE INPUT (ID INT PRIMARY KEY, VALUE INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT WITH (KEY_FORMAT='JSON') as SELECT 1 as k, value, count(1) AS ID FROM INPUT group by value, 1;
INSERT INTO `INPUT` (ID, VALUE) VALUES (10, 0);
INSERT INTO `INPUT` (ID, VALUE) VALUES (1666, 0);
INSERT INTO `INPUT` (ID, VALUE) VALUES (98, 0);
INSERT INTO `INPUT` (ID, VALUE) VALUES (98, 1);
INSERT INTO `INPUT` (ID, VALUE) VALUES (2, NULL);
INSERT INTO `INPUT` (ID) VALUES (2);
ASSERT VALUES `OUTPUT` (VALUE, K, ID) VALUES (0, 1, 1);
ASSERT VALUES `OUTPUT` (VALUE, K, ID) VALUES (0, 1, 2);
ASSERT VALUES `OUTPUT` (VALUE, K, ID) VALUES (0, 1, 3);
ASSERT VALUES `OUTPUT` (VALUE, K, ID) VALUES (0, 1, 2);
ASSERT VALUES `OUTPUT` (VALUE, K, ID) VALUES (1, 1, 1);
ASSERT table OUTPUT (VALUE INT PRIMARY KEY, K INT PRIMARY KEY, ID BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - single expression - key in projection more than once
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The projection contains a key column more than once: `NAME` and `NAME2`.
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='input',value_format='JSON');
CREATE TABLE OUTPUT AS SELECT NAME, NAME AS NAME2, COUNT(1) FROM INPUT GROUP BY NAME;
--@test: group-by - single expression - key missing from projection - with other column of same name
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the grouping expression NAME in its projection (eg, SELECT NAME...).
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='input',value_format='JSON');
CREATE TABLE OUTPUT AS SELECT COUNT(1) AS NAME FROM INPUT GROUP BY NAME;
--@test: group-by - single expression - key missing from projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the grouping expression NAME in its projection (eg, SELECT NAME...).
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='input',value_format='JSON');
CREATE TABLE OUTPUT AS SELECT COUNT(1) FROM INPUT GROUP BY NAME;
--@test: group-by - multiple expressions - single key missing from projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the grouping expression F1 in its projection (eg, SELECT F1...).
CREATE STREAM TEST (f1 INT KEY, f2 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f2, COUNT(*) FROM TEST GROUP BY f1, f2;
--@test: group-by - multiple expression - key in projection more than once
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The projection contains a key column more than once: `F1` and `F3`.
CREATE STREAM TEST (f1 INT KEY, f2 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1, f2, f1 AS F3, COUNT(*) FROM TEST GROUP BY f1, f2;
--@test: group-by - multiple expressions - all keys missing from projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the grouping expressions F1, F2 and F3 in its projection (eg, SELECT F1, F2, F3...).
CREATE STREAM TEST (f1 INT KEY, f2 INT, f3 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT COUNT(*) FROM TEST GROUP BY f1, f2, f3;
--@test: group-by - select * where all columns in group by
CREATE STREAM TEST (id INT KEY, id2 INT) WITH (kafka_topic='test_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT *, COUNT() FROM TEST GROUP BY id, id2;
INSERT INTO `TEST` (ID, ID2) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (ID, ID2, KSQL_COL_0) VALUES (1, 2, 1);

--@test: group-by - select * where not all columns in group by
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: ID2
CREATE STREAM TEST (id INT KEY, id2 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT *, COUNT() FROM TEST GROUP BY id;
--@test: group-by - create table as select COUNT(*) with no group by
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: CREATE TABLE AS SELECT statement does not support aggregate function [COUNT(ROWTIME)] without a GROUP BY clause.
CREATE STREAM TEST (id INT KEY, id2 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT COUNT(*) FROM TEST;
--@test: group-by - with key alias that clashes with value alias
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Duplicate value columns found in schema: `COUNT` BIGINT
CREATE STREAM TEST (ID INT KEY, data STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT DATA AS COUNT, COUNT(*) AS COUNT FROM TEST GROUP BY DATA;
--@test: group-by - map used in non-aggregate function in select when group by uses subscript
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: AS_VALUE(COL1)
CREATE STREAM INPUT (id INT KEY, col1 MAP<VARCHAR, VARCHAR>) WITH (kafka_topic='test_topic', value_format='json');
CREATE TABLE OUTPUT AS SELECT col1['foo'], AS_VALUE(col1) AS foo, COUNT(*) FROM input GROUP BY col1['foo'];
--@test: group-by - complex UDAF params
CREATE STREAM TEST (V0 INT KEY, V1 INT) WITH (kafka_topic='test_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT V0, V1, SUM(V0 + V1) AS SUM FROM TEST GROUP BY V0, V1;
INSERT INTO `TEST` (V0, V1) VALUES (0, 10);
INSERT INTO `TEST` (V0, V1) VALUES (1, 20);
INSERT INTO `TEST` (V0, V1) VALUES (0, 10);
ASSERT VALUES `OUTPUT` (V0, V1, SUM) VALUES (0, 10, 10);
ASSERT VALUES `OUTPUT` (V0, V1, SUM) VALUES (1, 20, 21);
ASSERT VALUES `OUTPUT` (V0, V1, SUM) VALUES (0, 10, 20);
ASSERT table OUTPUT (V0 INTEGER PRIMARY KEY, V1 INTEGER PRIMARY KEY, SUM INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - complex UDAF params matching GROUP BY
CREATE STREAM TEST (V0 INT KEY, V1 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT (V0 + V1) AS NEW_KEY, SUM(V0 + V1) AS SUM FROM TEST GROUP BY V0 + V1;
INSERT INTO `TEST` (V0, V1) VALUES (0, 10);
INSERT INTO `TEST` (V0, V1) VALUES (1, 20);
INSERT INTO `TEST` (V0, V1) VALUES (0, 10);
ASSERT VALUES `OUTPUT` (NEW_KEY, SUM) VALUES (10, 10);
ASSERT VALUES `OUTPUT` (NEW_KEY, SUM) VALUES (21, 21);
ASSERT VALUES `OUTPUT` (NEW_KEY, SUM) VALUES (10, 20);
ASSERT table OUTPUT (NEW_KEY INT PRIMARY KEY, SUM INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - complex UDAF params matching HAVING
CREATE STREAM TEST (V0 INT KEY, V1 INT) WITH (kafka_topic='test_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT SUM(V0 + V1) AS SUM, V0, V1 FROM TEST GROUP BY V0, V1 HAVING V0 + V1 <= 20;
INSERT INTO `TEST` (V0, V1) VALUES (0, 10);
INSERT INTO `TEST` (V0, V1) VALUES (1, 20);
INSERT INTO `TEST` (V0, V1) VALUES (0, 10);
ASSERT VALUES `OUTPUT` (V0, V1, SUM) VALUES (0, 10, 10);
ASSERT VALUES `OUTPUT` (V0, V1, SUM) VALUES (0, 10, 20);
ASSERT table OUTPUT (V0 INTEGER PRIMARY KEY, V1 INTEGER PRIMARY KEY, SUM INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - single expression with nulls
CREATE STREAM TEST (ID INT KEY, data STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT bad_udf(DATA), COUNT(*) FROM TEST GROUP BY bad_udf(DATA);
INSERT INTO `TEST` (ID, data) VALUES (0, 'd1');
INSERT INTO `TEST` (ID, data) VALUES (1, 'd2');
INSERT INTO `TEST` (ID, data) VALUES (2, 'd1');
INSERT INTO `TEST` (ID, data) VALUES (3, 'd2');
INSERT INTO `TEST` (ID, data) VALUES (4, 'd1');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('d2', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('d2', 2);
ASSERT table OUTPUT (KSQL_COL_0 STRING PRIMARY KEY, KSQL_COL_1 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - complex expressions
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: mismatched input 'AND' expecting {';'
CREATE STREAM TEST (ID INT KEY, data STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT COUNT(*) FROM TEST GROUP BY DATA AND ID;
--@test: group-by - multiple expressions with nulls
CREATE STREAM TEST (ID INT KEY, data STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT bad_udf(DATA), COUNT(*) FROM TEST GROUP BY bad_udf(DATA);
INSERT INTO `TEST` (ID, data) VALUES (0, 'd1');
INSERT INTO `TEST` (ID, data) VALUES (1, 'd2');
INSERT INTO `TEST` (ID, data) VALUES (2, 'd1');
INSERT INTO `TEST` (ID, data) VALUES (3, 'd2');
INSERT INTO `TEST` (ID, data) VALUES (4, 'd1');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('d2', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('d2', 2);
ASSERT table OUTPUT (KSQL_COL_0 STRING PRIMARY KEY, KSQL_COL_1 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - field (stream->table)
CREATE STREAM TEST (K STRING KEY, ignored VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT K, COUNT(*) FROM TEST GROUP BY K;
INSERT INTO `TEST` (K, IGNORED) VALUES ('d1', '-');
INSERT INTO `TEST` (K, IGNORED) VALUES ('d2', '-');
INSERT INTO `TEST` (K, IGNORED) VALUES ('d1', '-');
INSERT INTO `TEST` (K, IGNORED) VALUES ('d2', '-');
INSERT INTO `TEST` (K, IGNORED) VALUES ('d1', '-');
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0) VALUES ('d1', 1);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0) VALUES ('d2', 1);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0) VALUES ('d1', 2);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0) VALUES ('d2', 2);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0) VALUES ('d1', 3);
ASSERT table OUTPUT (K STRING PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - field (stream->table) - KAFKA
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Source(s) TEST are using the 'KAFKA' value format. This format does not yet support GROUP BY.
CREATE STREAM TEST (K STRING KEY, data VARCHAR) WITH (kafka_topic='test_topic', value_format='KAFKA');
CREATE TABLE OUTPUT WITH(value_format='DELIMITED') AS SELECT DATA, COUNT(*) FROM TEST GROUP BY DATA;
--@test: group-by - field (stream->table) - format - AVRO
CREATE STREAM TEST (K STRING KEY, ignored VARCHAR) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE OUTPUT AS SELECT K, COUNT(*) FROM TEST GROUP BY K;
INSERT INTO `TEST` (K, ROWTIME) VALUES ('d1', 1);
INSERT INTO `TEST` (K, ROWTIME) VALUES ('d2', 2);
INSERT INTO `TEST` (K, ROWTIME) VALUES ('d1', 3);
INSERT INTO `TEST` (K, ROWTIME) VALUES ('d2', 4);
INSERT INTO `TEST` (K, ROWTIME) VALUES ('d1', 5);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0, ROWTIME) VALUES ('d1', 1, 1);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0, ROWTIME) VALUES ('d2', 1, 2);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0, ROWTIME) VALUES ('d1', 2, 3);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0, ROWTIME) VALUES ('d2', 2, 4);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0, ROWTIME) VALUES ('d1', 3, 5);
ASSERT table OUTPUT (K STRING PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - field (stream->table) - format - JSON
CREATE STREAM TEST (K STRING KEY, ignored VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT K, COUNT(*) FROM TEST GROUP BY K;
INSERT INTO `TEST` (K, ROWTIME) VALUES ('d1', 1);
INSERT INTO `TEST` (K, ROWTIME) VALUES ('d2', 2);
INSERT INTO `TEST` (K, ROWTIME) VALUES ('d1', 3);
INSERT INTO `TEST` (K, ROWTIME) VALUES ('d2', 4);
INSERT INTO `TEST` (K, ROWTIME) VALUES ('d1', 5);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0, ROWTIME) VALUES ('d1', 1, 1);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0, ROWTIME) VALUES ('d2', 1, 2);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0, ROWTIME) VALUES ('d1', 2, 3);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0, ROWTIME) VALUES ('d2', 2, 4);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0, ROWTIME) VALUES ('d1', 3, 5);
ASSERT table OUTPUT (K STRING PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - field (stream->table) - format - PROTOBUF
CREATE STREAM TEST (K STRING KEY, ignored VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE TABLE OUTPUT AS SELECT K, COUNT(*) FROM TEST GROUP BY K;
INSERT INTO `TEST` (K, ROWTIME) VALUES ('d1', 1);
INSERT INTO `TEST` (K, ROWTIME) VALUES ('d2', 2);
INSERT INTO `TEST` (K, ROWTIME) VALUES ('d1', 3);
INSERT INTO `TEST` (K, ROWTIME) VALUES ('d2', 4);
INSERT INTO `TEST` (K, ROWTIME) VALUES ('d1', 5);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0, ROWTIME) VALUES ('d1', 1, 1);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0, ROWTIME) VALUES ('d2', 1, 2);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0, ROWTIME) VALUES ('d1', 2, 3);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0, ROWTIME) VALUES ('d2', 2, 4);
ASSERT VALUES `OUTPUT` (K, KSQL_COL_0, ROWTIME) VALUES ('d1', 3, 5);
ASSERT table OUTPUT (K STRING PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - int field (stream->table)
CREATE STREAM TEST (K STRING KEY, ID INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT ID, COUNT(*) FROM TEST GROUP BY ID;
INSERT INTO `TEST` (K, ID, ROWTIME) VALUES ('a', 1, 1);
INSERT INTO `TEST` (K, ID, ROWTIME) VALUES ('b', 2, 2);
INSERT INTO `TEST` (K, ID, ROWTIME) VALUES ('c', 1, 3);
INSERT INTO `TEST` (K, ID, ROWTIME) VALUES ('d', 2, 4);
INSERT INTO `TEST` (K, ID, ROWTIME) VALUES ('e', 1, 5);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0, ROWTIME) VALUES (1, 1, 1);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0, ROWTIME) VALUES (2, 1, 2);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0, ROWTIME) VALUES (1, 2, 3);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0, ROWTIME) VALUES (2, 2, 4);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0, ROWTIME) VALUES (1, 3, 5);
ASSERT table OUTPUT (ID INT PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - fields (stream->table)
CREATE STREAM TEST (ID INT KEY, f1 INT, f2 VARCHAR) WITH (kafka_topic='test_topic', format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1, f2, COUNT(*) FROM TEST GROUP BY f2, f1;
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
INSERT INTO `TEST` (ID, F1, F2) VALUES (2, 2, 'b');
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
INSERT INTO `TEST` (ID, F1, F2) VALUES (2, 2, 'b');
INSERT INTO `TEST` (ID, F1, F2) VALUES (3, 3, 'a');
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 2);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 2);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 3, 1);
ASSERT table OUTPUT (F2 STRING PRIMARY KEY, F1 INTEGER PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - fields used in expression
CREATE STREAM TEST (K STRING KEY, f1 INT, f2 INT) WITH (kafka_topic='test_topic', format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT F1, F2, f1 / f2, COUNT(*) FROM TEST GROUP BY f1, f2;
INSERT INTO `TEST` (F1, F2) VALUES (4, 2);
INSERT INTO `TEST` (F1, F2) VALUES (9, 3);
INSERT INTO `TEST` (F1, F2) VALUES (9, 3);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1) VALUES (4, 2, 2, 1);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1) VALUES (9, 3, 3, 1);
ASSERT VALUES `OUTPUT` (F1, F2, KSQL_COL_0, KSQL_COL_1) VALUES (9, 3, 3, 2);
ASSERT table OUTPUT (F1 INTEGER PRIMARY KEY, F2 INTEGER PRIMARY KEY, KSQL_COL_0 INTEGER, KSQL_COL_1 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - fields (stream->table) - format - AVRO
CREATE STREAM TEST (ID INT KEY, f1 INT, f2 VARCHAR) WITH (kafka_topic='test_topic', key_format='JSON', value_format='AVRO');
CREATE TABLE OUTPUT AS SELECT f1, f2, COUNT(*) FROM TEST GROUP BY f2, f1;
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
INSERT INTO `TEST` (ID, F1, F2) VALUES (2, 2, 'b');
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
INSERT INTO `TEST` (ID, F1, F2) VALUES (2, 2, 'b');
INSERT INTO `TEST` (ID, F1, F2) VALUES (3, 3, 'a');
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 2);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 2);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 3, 1);
ASSERT table OUTPUT (F2 STRING PRIMARY KEY, F1 INTEGER PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - fields (stream->table) - format - JSON
CREATE STREAM TEST (ID INT KEY, f1 INT, f2 VARCHAR) WITH (kafka_topic='test_topic', key_format='JSON', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT f1, f2, COUNT(*) FROM TEST GROUP BY f2, f1;
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
INSERT INTO `TEST` (ID, F1, F2) VALUES (2, 2, 'b');
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
INSERT INTO `TEST` (ID, F1, F2) VALUES (2, 2, 'b');
INSERT INTO `TEST` (ID, F1, F2) VALUES (3, 3, 'a');
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 2);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 2);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 3, 1);
ASSERT table OUTPUT (F2 STRING PRIMARY KEY, F1 INTEGER PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - fields (stream->table) - format - PROTOBUF
CREATE STREAM TEST (ID INT KEY, f1 INT, f2 VARCHAR) WITH (kafka_topic='test_topic', key_format='JSON', value_format='PROTOBUF');
CREATE TABLE OUTPUT AS SELECT f1, f2, COUNT(*) FROM TEST GROUP BY f2, f1;
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
INSERT INTO `TEST` (ID, F1, F2) VALUES (2, 2, 'b');
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
INSERT INTO `TEST` (ID, F1, F2) VALUES (2, 2, 'b');
INSERT INTO `TEST` (ID, F1, F2) VALUES (3, 3, 'a');
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 2);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 2);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 3, 1);
ASSERT table OUTPUT (F2 STRING PRIMARY KEY, F1 INTEGER PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - with groupings (stream->table)
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: line 2:74: missing ')' at ','
CREATE STREAM TEST (ID INT KEY, f1 INT, f2 VARCHAR, f3 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1, f2, COUNT(*) FROM TEST GROUP BY f3, (f2, f1);
--@test: group-by - duplicate expressions
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Duplicate GROUP BY expression: TEST.DATA
CREATE STREAM TEST (ID INT KEY, data STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT COUNT(*) FROM TEST GROUP BY DATA, TEST.DATA;
--@test: group-by - with single grouping set (stream->table)
CREATE STREAM TEST (ID INT KEY, f1 INT, f2 VARCHAR, f3 INT) WITH (kafka_topic='test_topic', format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1, f2, f3, COUNT(*) FROM TEST GROUP BY (f3, f2, f1);
INSERT INTO `TEST` (ID, F1, F2, F3) VALUES (1, 1, 'a', -1);
INSERT INTO `TEST` (ID, F1, F2, F3) VALUES (2, 2, 'b', -2);
INSERT INTO `TEST` (ID, F1, F2, F3) VALUES (1, 1, 'a', -1);
INSERT INTO `TEST` (ID, F1, F2, F3) VALUES (2, 2, 'b', -2);
INSERT INTO `TEST` (ID, F1, F2, F3) VALUES (3, 3, 'a', -3);
ASSERT VALUES `OUTPUT` (F3, F2, F1, KSQL_COL_0) VALUES ("-1", 'a', 1, 1);
ASSERT VALUES `OUTPUT` (F3, F2, F1, KSQL_COL_0) VALUES ("-2", 'b', 2, 1);
ASSERT VALUES `OUTPUT` (F3, F2, F1, KSQL_COL_0) VALUES ("-1", 'a', 1, 2);
ASSERT VALUES `OUTPUT` (F3, F2, F1, KSQL_COL_0) VALUES ("-2", 'b', 2, 2);
ASSERT VALUES `OUTPUT` (F3, F2, F1, KSQL_COL_0) VALUES ("-3", 'a', 3, 1);
ASSERT table OUTPUT (F3 INTEGER PRIMARY KEY, F2 STRING PRIMARY KEY, F1 INTEGER PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - fields (table->table)
CREATE TABLE TEST (ID INT PRIMARY KEY, f1 INT, f2 VARCHAR) WITH (kafka_topic='test_topic', format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1, f2, COUNT(*) FROM TEST GROUP BY f2, f1;
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
INSERT INTO `TEST` (ID, F1, F2) VALUES (2, 2, 'b');
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'b');
INSERT INTO `TEST` (ID) VALUES (2);
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 0);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 1, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 0);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 1, 0);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 1);
ASSERT table OUTPUT (F2 STRING PRIMARY KEY, F1 INTEGER PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - fields - copied into value (table->table)
CREATE TABLE TEST (ID INT PRIMARY KEY, f1 INT, f2 VARCHAR) WITH (kafka_topic='test_topic', format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1, f2, AS_VALUE(f1) AS F3, AS_VALUE(F2) AS F4, COUNT(*) FROM TEST GROUP BY f2, f1;
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
INSERT INTO `TEST` (ID, F1, F2) VALUES (2, 2, 'b');
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'b');
INSERT INTO `TEST` (ID) VALUES (2);
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
ASSERT VALUES `OUTPUT` (F2, F1, F3, F4, KSQL_COL_0) VALUES ('a', 1, 1, 'a', 1);
ASSERT VALUES `OUTPUT` (F2, F1, F3, F4, KSQL_COL_0) VALUES ('b', 2, 2, 'b', 1);
ASSERT VALUES `OUTPUT` (F2, F1, F3, F4, KSQL_COL_0) VALUES ('a', 1, 1, 'a', 0);
ASSERT VALUES `OUTPUT` (F2, F1, F3, F4, KSQL_COL_0) VALUES ('b', 1, 1, 'b', 1);
ASSERT VALUES `OUTPUT` (F2, F1, F3, F4, KSQL_COL_0) VALUES ('b', 2, 2, 'b', 0);
ASSERT VALUES `OUTPUT` (F2, F1, F3, F4, KSQL_COL_0) VALUES ('b', 1, 1, 'b', 0);
ASSERT VALUES `OUTPUT` (F2, F1, F3, F4, KSQL_COL_0) VALUES ('a', 1, 1, 'a', 1);
ASSERT table OUTPUT (F2 STRING PRIMARY KEY, F1 INTEGER PRIMARY KEY, F3 INT, F4 STRING, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - fields (table->table) - format - AVRO
CREATE TABLE TEST (ID INT PRIMARY KEY, f1 INT, f2 VARCHAR) WITH (kafka_topic='test_topic', key_format='JSON', value_format='AVRO');
CREATE TABLE OUTPUT AS SELECT f1, f2, COUNT(*) FROM TEST GROUP BY f2, f1;
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
INSERT INTO `TEST` (ID, F1, F2) VALUES (2, 2, 'b');
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'b');
INSERT INTO `TEST` (ID) VALUES (2);
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 0);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 1, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 0);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 1, 0);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 1);
ASSERT table OUTPUT (F2 STRING PRIMARY KEY, F1 INTEGER PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - fields (table->table) - format - JSON
CREATE TABLE TEST (ID INT PRIMARY KEY, f1 INT, f2 VARCHAR) WITH (kafka_topic='test_topic', key_format='JSON', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT f1, f2, COUNT(*) FROM TEST GROUP BY f2, f1;
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
INSERT INTO `TEST` (ID, F1, F2) VALUES (2, 2, 'b');
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'b');
INSERT INTO `TEST` (ID) VALUES (2);
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 0);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 1, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 0);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 1, 0);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 1);
ASSERT table OUTPUT (F2 STRING PRIMARY KEY, F1 INTEGER PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - fields (table->table) - format - PROTOBUF
CREATE TABLE TEST (ID INT PRIMARY KEY, f1 INT, f2 VARCHAR) WITH (kafka_topic='test_topic', key_format='JSON', value_format='PROTOBUF');
CREATE TABLE OUTPUT AS SELECT f1, f2, COUNT(*) FROM TEST GROUP BY f2, f1;
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
INSERT INTO `TEST` (ID, F1, F2) VALUES (2, 2, 'b');
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'b');
INSERT INTO `TEST` (ID) VALUES (2);
INSERT INTO `TEST` (ID, F1, F2) VALUES (1, 1, 'a');
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 0);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 1, 1);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 2, 0);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('b', 1, 0);
ASSERT VALUES `OUTPUT` (F2, F1, KSQL_COL_0) VALUES ('a', 1, 1);
ASSERT table OUTPUT (F2 STRING PRIMARY KEY, F1 INTEGER PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - field with re-key (stream->table)
CREATE STREAM TEST (K STRING KEY, data VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT DATA, COUNT(*) FROM TEST GROUP BY DATA;
INSERT INTO `TEST` (DATA) VALUES ('d1');
INSERT INTO `TEST` (DATA) VALUES ('d2');
INSERT INTO `TEST` (DATA) VALUES ('d1');
INSERT INTO `TEST` (DATA) VALUES ('d2');
INSERT INTO `TEST` (DATA) VALUES ('d1');
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d1', 1);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d2', 1);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d1', 2);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d2', 2);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d1', 3);
ASSERT table OUTPUT (DATA STRING PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - double field with re-key (stream->table)
CREATE STREAM TEST (K STRING KEY, data double) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT DATA, COUNT(*) FROM TEST GROUP BY DATA;
INSERT INTO `TEST` (DATA) VALUES (0.1);
INSERT INTO `TEST` (DATA) VALUES (0.2);
INSERT INTO `TEST` (DATA) VALUES (0.1);
INSERT INTO `TEST` (DATA) VALUES (0.2);
INSERT INTO `TEST` (DATA) VALUES (0.1);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES (0.1, 1);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES (0.2, 1);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES (0.1, 2);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES (0.2, 2);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES (0.1, 3);
ASSERT table OUTPUT (DATA DOUBLE PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - field with re-key (stream->table) - format - AVRO
CREATE STREAM TEST (K STRING KEY, data VARCHAR) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE OUTPUT AS SELECT DATA, COUNT(*) FROM TEST GROUP BY DATA;
INSERT INTO `TEST` (DATA) VALUES ('d1');
INSERT INTO `TEST` (DATA) VALUES ('d2');
INSERT INTO `TEST` (DATA) VALUES ('d1');
INSERT INTO `TEST` (DATA) VALUES ('d2');
INSERT INTO `TEST` (DATA) VALUES ('d1');
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d1', 1);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d2', 1);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d1', 2);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d2', 2);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d1', 3);
ASSERT table OUTPUT (DATA STRING PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - field with re-key (stream->table) - format - JSON
CREATE STREAM TEST (K STRING KEY, data VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT DATA, COUNT(*) FROM TEST GROUP BY DATA;
INSERT INTO `TEST` (DATA) VALUES ('d1');
INSERT INTO `TEST` (DATA) VALUES ('d2');
INSERT INTO `TEST` (DATA) VALUES ('d1');
INSERT INTO `TEST` (DATA) VALUES ('d2');
INSERT INTO `TEST` (DATA) VALUES ('d1');
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d1', 1);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d2', 1);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d1', 2);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d2', 2);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d1', 3);
ASSERT table OUTPUT (DATA STRING PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - field with re-key (stream->table) - format - PROTOBUF
CREATE STREAM TEST (K STRING KEY, data VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE TABLE OUTPUT AS SELECT DATA, COUNT(*) FROM TEST GROUP BY DATA;
INSERT INTO `TEST` (DATA) VALUES ('d1');
INSERT INTO `TEST` (DATA) VALUES ('d2');
INSERT INTO `TEST` (DATA) VALUES ('d1');
INSERT INTO `TEST` (DATA) VALUES ('d2');
INSERT INTO `TEST` (DATA) VALUES ('d1');
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d1', 1);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d2', 1);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d1', 2);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d2', 2);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d1', 3);
ASSERT table OUTPUT (DATA STRING PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - field with re-key (table->table)
CREATE TABLE TEST (ID INT PRIMARY KEY, user INT, region VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT region, COUNT(*) FROM TEST GROUP BY region;
INSERT INTO `TEST` (ID, USER, REGION) VALUES (1, 1, 'r0');
INSERT INTO `TEST` (ID, USER, REGION) VALUES (2, 2, 'r1');
INSERT INTO `TEST` (ID, USER, REGION) VALUES (3, 3, 'r0');
INSERT INTO `TEST` (ID) VALUES (1);
INSERT INTO `TEST` (ID, USER, REGION) VALUES (2, 2, 'r0');
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0) VALUES ('r0', 1);
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0) VALUES ('r1', 1);
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0) VALUES ('r0', 2);
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0) VALUES ('r0', 1);
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0) VALUES ('r1', 0);
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0) VALUES ('r0', 2);
ASSERT table OUTPUT (REGION STRING PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - with aggregate arithmetic (stream->table)
CREATE STREAM TEST (K STRING KEY, data VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT DATA, COUNT(*)*2 FROM TEST GROUP BY DATA;
INSERT INTO `TEST` (DATA) VALUES ('d1');
INSERT INTO `TEST` (DATA) VALUES ('d2');
INSERT INTO `TEST` (DATA) VALUES ('d1');
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d1', 2);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d2', 2);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0) VALUES ('d1', 4);
ASSERT table OUTPUT (DATA STRING PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - with aggregate arithmetic (table->table)
CREATE TABLE TEST (ID INT PRIMARY KEY, user INT, region VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT region, COUNT(*) * 2 FROM TEST GROUP BY region;
INSERT INTO `TEST` (ID, USER, REGION) VALUES (1, 1, 'r0');
INSERT INTO `TEST` (ID, USER, REGION) VALUES (2, 2, 'r1');
INSERT INTO `TEST` (ID, USER, REGION) VALUES (3, 3, 'r0');
INSERT INTO `TEST` (ID) VALUES (1);
INSERT INTO `TEST` (ID, USER, REGION) VALUES (2, 2, 'r0');
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0) VALUES ('r0', 2);
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0) VALUES ('r1', 2);
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0) VALUES ('r0', 4);
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0) VALUES ('r0', 2);
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0) VALUES ('r1', 0);
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0) VALUES ('r0', 4);
ASSERT table OUTPUT (REGION STRING PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - with aggregate arithmetic involving source field (stream->table)
CREATE STREAM TEST (K STRING KEY, ITEM INT, COST INT) WITH (kafka_topic='test_topic', format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT ITEM, COST, COST * COUNT() FROM TEST GROUP BY ITEM, COST;
INSERT INTO `TEST` (ITEM, COST) VALUES (1, 10);
INSERT INTO `TEST` (ITEM, COST) VALUES (1, 20);
INSERT INTO `TEST` (ITEM, COST) VALUES (2, 30);
INSERT INTO `TEST` (ITEM, COST) VALUES (1, 10);
ASSERT VALUES `OUTPUT` (ITEM, COST, KSQL_COL_0) VALUES (1, 10, 10);
ASSERT VALUES `OUTPUT` (ITEM, COST, KSQL_COL_0) VALUES (1, 20, 20);
ASSERT VALUES `OUTPUT` (ITEM, COST, KSQL_COL_0) VALUES (2, 30, 30);
ASSERT VALUES `OUTPUT` (ITEM, COST, KSQL_COL_0) VALUES (1, 10, 20);

--@test: group-by - with aggregate arithmetic involving source field (table->table)
CREATE TABLE TEST (ID INT PRIMARY KEY, f0 INT, f1 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f0, f0 * SUM(f1) FROM TEST GROUP BY f0;
INSERT INTO `TEST` (ID, F0, F1) VALUES (2, 2, 10);
INSERT INTO `TEST` (ID, F0, F1) VALUES (2, 2, 20);
INSERT INTO `TEST` (ID, F0, F1) VALUES (2, 2, 30);
INSERT INTO `TEST` (ID) VALUES (2);
ASSERT VALUES `OUTPUT` (F0, KSQL_COL_0) VALUES (2, 20);
ASSERT VALUES `OUTPUT` (F0, KSQL_COL_0) VALUES (2, 0);
ASSERT VALUES `OUTPUT` (F0, KSQL_COL_0) VALUES (2, 40);
ASSERT VALUES `OUTPUT` (F0, KSQL_COL_0) VALUES (2, 0);
ASSERT VALUES `OUTPUT` (F0, KSQL_COL_0) VALUES (2, 60);
ASSERT VALUES `OUTPUT` (F0, KSQL_COL_0) VALUES (2, 0);

--@test: group-by - with aggregate arithmetic involving source field not in group by (table->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: (F1 * SUM(F2))
Either add the column(s) to the GROUP BY or remove them from the SELECT.
CREATE TABLE TEST (ID INT PRIMARY KEY, f0 INT, f1 INT, f2 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1 * SUM(f2) FROM TEST GROUP BY f0;
--@test: group-by - function (stream->table)
CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT SUBSTRING(source, 0, 2), COUNT(*) FROM TEST GROUP BY SUBSTRING(source, 0, 2);
INSERT INTO `TEST` (SOURCE) VALUES ('some string');
INSERT INTO `TEST` (SOURCE) VALUES ('another string');
INSERT INTO `TEST` (SOURCE) VALUES ('some string again');
INSERT INTO `TEST` (SOURCE) VALUES ('another string again');
INSERT INTO `TEST` (SOURCE) VALUES ('some other string');
INSERT INTO `TEST` (SOURCE) VALUES ('the final string');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('so', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('an', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('so', 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('an', 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('so', 3);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('th', 1);
ASSERT table OUTPUT (KSQL_COL_0 STRING PRIMARY KEY, KSQL_COL_1 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - function (table->table)
CREATE TABLE TEST (ID INT PRIMARY KEY, user INT, region VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT SUBSTRING(region, 7, 2), COUNT(*) FROM TEST GROUP BY SUBSTRING(region, 7, 2);
INSERT INTO `TEST` (ID, USER, REGION) VALUES (1, 1, 'prefixr0');
INSERT INTO `TEST` (ID, USER, REGION) VALUES (2, 2, 'prefixr1');
INSERT INTO `TEST` (ID, USER, REGION) VALUES (3, 3, 'prefixr0');
INSERT INTO `TEST` (ID) VALUES (1);
INSERT INTO `TEST` (ID, USER, REGION) VALUES (2, 2, 'prefixr0');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('r0', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('r1', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('r0', 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('r0', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('r1', 0);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('r0', 2);
ASSERT table OUTPUT (KSQL_COL_0 STRING PRIMARY KEY, KSQL_COL_1 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - int function (table->table)
CREATE TABLE TEST (K STRING PRIMARY KEY, region VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT LEN(region), COUNT(*) FROM TEST GROUP BY LEN(region);
INSERT INTO `TEST` (K, REGION) VALUES ('1', 'usa');
INSERT INTO `TEST` (K, REGION) VALUES ('2', 'eu');
INSERT INTO `TEST` (K, REGION) VALUES ('3', 'usa');
INSERT INTO `TEST` (K) VALUES ('1');
INSERT INTO `TEST` (K, REGION) VALUES ('2', 'usa');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (3, 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (2, 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (3, 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (3, 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (2, 0);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (3, 2);
ASSERT table OUTPUT (KSQL_COL_0 INT PRIMARY KEY, KSQL_COL_1 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - function with select field that is a subset of group by (stream->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: SUBSTRING(SOURCE, 0, 1)
CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT SUBSTRING(source, 0, 1) AS Thing, COUNT(*) FROM TEST GROUP BY SUBSTRING(source, 0, 2);
--@test: group-by - function with select field that is a subset of group by (table->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: SUBSTRING(REGION, 7, 1)
CREATE TABLE TEST (ID INT PRIMARY KEY, user INT, region VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT SUBSTRING(region, 7, 1), COUNT(*) FROM TEST GROUP BY SUBSTRING(region, 7, 2);
--@test: group-by - function with select field that is a superset of group by (stream->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: SUBSTRING(SOURCE, 0, 3)
CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT SUBSTRING(source, 0, 3), COUNT(*) FROM TEST GROUP BY SUBSTRING(source, 0, 2);
--@test: group-by - function with select field that is a superset of group by (table->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: SUBSTRING(REGION, 7, 3)
CREATE TABLE TEST (K INT PRIMARY KEY, user INT, region VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT SUBSTRING(region, 7, 3), COUNT(*) FROM TEST GROUP BY SUBSTRING(region, 7, 2);
--@test: group-by - function with having field that is a subset of group by (stream->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate HAVING expression not part of GROUP BY: SOURCE
CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT SUBSTRING(source, 0, 2) AS Thing, COUNT(*) FROM TEST GROUP BY SUBSTRING(source, 0, 2) HAVING LEN(source) < 2;
--@test: group-by - json field (stream->table)
CREATE STREAM TEST (data STRUCT<field VARCHAR>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT data->field AS FIELD, COUNT(*) AS COUNT FROM TEST GROUP BY data->field;
INSERT INTO `TEST` (data) VALUES (STRUCT(field:='Something'));
INSERT INTO `TEST` (data) VALUES (STRUCT(field:='Something Else'));
INSERT INTO `TEST` (data) VALUES (STRUCT());
INSERT INTO `TEST` (data) VALUES (STRUCT(field:='Something'));
INSERT INTO `TEST` (data) VALUES (STRUCT());
ASSERT VALUES `OUTPUT` (FIELD, COUNT) VALUES ('Something', 1);
ASSERT VALUES `OUTPUT` (FIELD, COUNT) VALUES ('Something Else', 1);
ASSERT VALUES `OUTPUT` (FIELD, COUNT) VALUES ('Something', 2);

--@test: group-by - int json field (stream->table)
CREATE STREAM TEST (data STRUCT<field INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT data->field, COUNT(*) AS COUNT FROM TEST GROUP BY data->field;
INSERT INTO `TEST` (data) VALUES (STRUCT(field:=1));
INSERT INTO `TEST` (data) VALUES (STRUCT(field:=2));
INSERT INTO `TEST` (data) VALUES (STRUCT());
INSERT INTO `TEST` (data) VALUES (STRUCT(field:=1));
INSERT INTO `TEST` (data) VALUES (STRUCT());
ASSERT VALUES `OUTPUT` (FIELD, COUNT) VALUES (1, 1);
ASSERT VALUES `OUTPUT` (FIELD, COUNT) VALUES (2, 1);
ASSERT VALUES `OUTPUT` (FIELD, COUNT) VALUES (1, 2);

--@test: group-by - ROWTIME (stream->table)
CREATE STREAM TEST (K STRING KEY, ignored VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT ROWTIME AS RT, COUNT(*) FROM TEST GROUP BY ROWTIME;
INSERT INTO `TEST` (IGNORED, ROWTIME) VALUES ('-', 10);
ASSERT VALUES `OUTPUT` (RT, KSQL_COL_0, ROWTIME) VALUES (10, 1, 10);

--@test: group-by - constant (stream->table)
CREATE STREAM TEST (K STRING KEY, ignored VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT 1, COUNT(*) FROM TEST GROUP BY 1;
INSERT INTO `TEST` (IGNORED) VALUES ('-');
INSERT INTO `TEST` (IGNORED) VALUES ('-');
INSERT INTO `TEST` (IGNORED) VALUES ('-');
INSERT INTO `TEST` (IGNORED) VALUES ('-');
INSERT INTO `TEST` (IGNORED) VALUES ('-');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 3);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 4);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 5);

--@test: group-by - constant (table->table)
CREATE TABLE TEST (K INT PRIMARY KEY, user INT, region VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT 1, COUNT(*) FROM TEST GROUP BY 1;
INSERT INTO `TEST` (K, USER, REGION) VALUES (1, 1, 'r0');
INSERT INTO `TEST` (K, USER, REGION) VALUES (2, 2, 'r1');
INSERT INTO `TEST` (K, USER, REGION) VALUES (3, 3, 'r0');
INSERT INTO `TEST` (K) VALUES (1);
INSERT INTO `TEST` (K, USER, REGION) VALUES (2, 2, 'r0');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 3);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 2);

--@test: group-by - field with field used in function in projection (stream->table)
CREATE STREAM TEST (K STRING KEY, f1 VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1, SUBSTRING(f1, 0, 1), COUNT(*) FROM TEST GROUP BY f1;
INSERT INTO `TEST` (F1) VALUES ('one');
INSERT INTO `TEST` (F1) VALUES ('two');
INSERT INTO `TEST` (F1) VALUES ('three');
INSERT INTO `TEST` (F1) VALUES ('one');
INSERT INTO `TEST` (F1) VALUES ('five');
ASSERT VALUES `OUTPUT` (F1, KSQL_COL_0, KSQL_COL_1) VALUES ('one', 'o', 1);
ASSERT VALUES `OUTPUT` (F1, KSQL_COL_0, KSQL_COL_1) VALUES ('two', 't', 1);
ASSERT VALUES `OUTPUT` (F1, KSQL_COL_0, KSQL_COL_1) VALUES ('three', 't', 1);
ASSERT VALUES `OUTPUT` (F1, KSQL_COL_0, KSQL_COL_1) VALUES ('one', 'o', 2);
ASSERT VALUES `OUTPUT` (F1, KSQL_COL_0, KSQL_COL_1) VALUES ('five', 'f', 1);

--@test: group-by - field with field used in function in projection (table->table)
CREATE TABLE TEST (ID INT PRIMARY KEY, user INT, region VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT region, SUBSTRING(region, 2, 1), COUNT(*) FROM TEST GROUP BY region;
INSERT INTO `TEST` (ID, USER, REGION) VALUES (1, 1, 'r0');
INSERT INTO `TEST` (ID, USER, REGION) VALUES (2, 2, 'r1');
INSERT INTO `TEST` (ID, USER, REGION) VALUES (3, 3, 'r0');
INSERT INTO `TEST` (ID) VALUES (1);
INSERT INTO `TEST` (ID, USER, REGION) VALUES (2, 2, 'r0');
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0, KSQL_COL_1) VALUES ('r0', '0', 1);
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0, KSQL_COL_1) VALUES ('r1', '1', 1);
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0, KSQL_COL_1) VALUES ('r0', '0', 2);
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0, KSQL_COL_1) VALUES ('r0', '0', 1);
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0, KSQL_COL_1) VALUES ('r1', '1', 0);
ASSERT VALUES `OUTPUT` (REGION, KSQL_COL_0, KSQL_COL_1) VALUES ('r0', '0', 2);

--@test: group-by - string concat using + op (stream->table)
CREATE STREAM TEST (K STRING KEY, f1 VARCHAR, f2 VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f2 + f1, COUNT(*) FROM TEST GROUP BY f2 + f1;
INSERT INTO `TEST` (F1, F2) VALUES ('1', 'a');
INSERT INTO `TEST` (F1, F2) VALUES ('2', 'b');
INSERT INTO `TEST` (F1, F2) VALUES ('1', 'a');
INSERT INTO `TEST` (F1, F2) VALUES ('2', 'b');
INSERT INTO `TEST` (F1, F2) VALUES ('3', 'a');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('a1', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('b2', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('a1', 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('b2', 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('a3', 1);

--@test: group-by - string concat using + op (table->table)
CREATE TABLE TEST (K STRING PRIMARY KEY, user INT, subregion VARCHAR, region VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT region + subregion, COUNT(*) FROM TEST GROUP BY region + subregion;
INSERT INTO `TEST` (K, USER, SUBREGION, REGION) VALUES ('1', 1, 'a', 'r0');
INSERT INTO `TEST` (K, USER, SUBREGION, REGION) VALUES ('2', 2, 'a', 'r1');
INSERT INTO `TEST` (K, USER, SUBREGION, REGION) VALUES ('3', 3, 'a', 'r0');
INSERT INTO `TEST` (K, USER, SUBREGION, REGION) VALUES ('4', 4, 'b', 'r0');
INSERT INTO `TEST` (K) VALUES ('1');
INSERT INTO `TEST` (K, USER, SUBREGION, REGION) VALUES ('2', 2, 'a', 'r0');
INSERT INTO `TEST` (K, USER, SUBREGION, REGION) VALUES ('2', 2, 'b', 'r1');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('r0a', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('r1a', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('r0a', 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('r0b', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('r0a', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('r1a', 0);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('r0a', 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('r0a', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('r1b', 1);

--@test: group-by - string concat using + op with projection field in wrong order (stream->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: (F1 + F2)
CREATE STREAM TEST (K STRING KEY, f1 VARCHAR, f2 VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1 + f2, COUNT(*) FROM TEST GROUP BY f2 + f1;
--@test: group-by - string concat using + op with projection field in wrong order (table->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: (SUBREGION + REGION)
CREATE TABLE TEST (K STRING PRIMARY KEY, user INT, subregion VARCHAR, region VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT subregion + region, COUNT(*) FROM TEST GROUP BY region + subregion;
--@test: group-by - string concat with separate fields in projection (stream->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: F1, F2
CREATE STREAM TEST (K STRING KEY, f1 VARCHAR, f2 VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1, f2, COUNT(*) FROM TEST GROUP BY f2 + f1;
--@test: group-by - string concat with separate fields in projection (table->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: SUBREGION, REGION
CREATE TABLE TEST (K STRING PRIMARY KEY, user INT, subregion VARCHAR, region VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT subregion, region, COUNT(*) FROM TEST GROUP BY region + subregion;
--@test: group-by - arithmetic binary expression with projection in-order & non-commutative group by (stream->table)
CREATE STREAM TEST (K STRING KEY, f1 INT, f2 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f2 - f1, COUNT(*) FROM TEST GROUP BY f2 - f1;
INSERT INTO `TEST` (F1, F2) VALUES (1, 2);
INSERT INTO `TEST` (F1, F2) VALUES (2, 3);
INSERT INTO `TEST` (F1, F2) VALUES (2, 4);
INSERT INTO `TEST` (F1, F2) VALUES (6, 8);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (2, 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (2, 2);

--@test: group-by - arithmetic binary expression with projection in-order & non-commutative group by (table->table)
CREATE TABLE TEST (K STRING PRIMARY KEY, f0 INT, f1 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f0 - f1, COUNT(*) FROM TEST GROUP BY f0 - f1;
INSERT INTO `TEST` (K, F0, F1) VALUES ('1', 1, 0);
INSERT INTO `TEST` (K, F0, F1) VALUES ('2', 2, 1);
INSERT INTO `TEST` (K, F0, F1) VALUES ('3', 3, 1);
INSERT INTO `TEST` (K) VALUES ('1');
INSERT INTO `TEST` (K, F0, F1) VALUES ('2', 4, 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (2, 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (1, 0);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (2, 2);

--@test: group-by - arithmetic binary expression with projection out-of-order & non-commutative group by (stream->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: (F1 - F2)
CREATE STREAM TEST (K STRING KEY, f1 INT, f2 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1 - f2, COUNT(*) FROM TEST GROUP BY f2 - f1;
--@test: group-by - arithmetic binary expression with projection out-of-order & non-commutative group by (table->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: (F1 - F0)
CREATE TABLE TEST (K STRING PRIMARY KEY, f0 INT, f1 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1 - f0, COUNT(*) FROM TEST GROUP BY f0 - f1;
--@test: group-by - with having expression (stream->table)
CREATE STREAM TEST (ID INT KEY, f1 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1, COUNT(*) FROM TEST GROUP BY f1 HAVING SUM(f1) > 1;
INSERT INTO `TEST` (ID, F1) VALUES (1, 1);
INSERT INTO `TEST` (ID, F1) VALUES (2, 2);
INSERT INTO `TEST` (ID, F1) VALUES (1, 1);
INSERT INTO `TEST` (ID, F1) VALUES (2, 2);
INSERT INTO `TEST` (ID, F1) VALUES (3, 3);
ASSERT VALUES `OUTPUT` (F1, KSQL_COL_0) VALUES (2, 1);
ASSERT VALUES `OUTPUT` (F1, KSQL_COL_0) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (F1, KSQL_COL_0) VALUES (2, 2);
ASSERT VALUES `OUTPUT` (F1, KSQL_COL_0) VALUES (3, 1);

--@test: group-by - with having expression (table->table)
CREATE TABLE TEST (K STRING PRIMARY KEY, f0 INT, f1 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1, SUM(f0) FROM TEST GROUP BY f1 HAVING COUNT(f1) > 0;
INSERT INTO `TEST` (K, F0, F1, ROWTIME) VALUES ('1', 1, 0, 1);
INSERT INTO `TEST` (K, F0, F1, ROWTIME) VALUES ('2', 2, 1, 2);
INSERT INTO `TEST` (K, ROWTIME) VALUES ('1', 3);
INSERT INTO `TEST` (K, F0, F1, ROWTIME) VALUES ('3', 3, 0, 4);
INSERT INTO `TEST` (K, F0, F1, ROWTIME) VALUES ('2', 2, 0, 5);
ASSERT VALUES `OUTPUT` (F1, KSQL_COL_0, ROWTIME) VALUES (0, 1, 1);
ASSERT VALUES `OUTPUT` (F1, KSQL_COL_0, ROWTIME) VALUES (1, 2, 2);
ASSERT VALUES `OUTPUT` (F1, ROWTIME) VALUES (0, 3);
ASSERT VALUES `OUTPUT` (F1, KSQL_COL_0, ROWTIME) VALUES (0, 3, 4);
ASSERT VALUES `OUTPUT` (F1, ROWTIME) VALUES (1, 5);
ASSERT VALUES `OUTPUT` (F1, KSQL_COL_0, ROWTIME) VALUES (0, 5, 5);

--@test: group-by - with multiple having expressions (stream->table)
CREATE STREAM TEST (K STRING KEY, f1 INT, f2 VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1, COUNT(f1) FROM TEST GROUP BY f1 HAVING COUNT(f1) > 1 AND f1=1;
INSERT INTO `TEST` (K, F1, F2) VALUES ('0', 1, 'a');
INSERT INTO `TEST` (K, F1, F2) VALUES ('0', 2, 'b');
INSERT INTO `TEST` (K, F1, F2) VALUES ('0', 1, 'test');
INSERT INTO `TEST` (K, F1, F2) VALUES ('0', 2, 'test');
INSERT INTO `TEST` (K, F1, F2) VALUES ('0', 2, 'test');
INSERT INTO `TEST` (K, F1, F2) VALUES ('0', 1, 'test');
ASSERT VALUES `OUTPUT` (F1, KSQL_COL_0) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (F1, KSQL_COL_0) VALUES (1, 3);

--@test: group-by - with having expression on non-group-by field (stream->table)
CREATE STREAM TEST (K STRING KEY, f1 INT, f2 VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f2, COUNT(*) FROM TEST GROUP BY f2 HAVING SUM(f1) > 10;
INSERT INTO `TEST` (K, F1, F2) VALUES ('-', 5, 'a');
INSERT INTO `TEST` (K, F1, F2) VALUES ('-', 10, 'b');
INSERT INTO `TEST` (K, F1, F2) VALUES ('-', 6, 'a');
INSERT INTO `TEST` (K, F1, F2) VALUES ('-', 1, 'b');
INSERT INTO `TEST` (K, F1, F2) VALUES ('-', -1, 'a');
INSERT INTO `TEST` (K, F1, F2) VALUES ('-', 1, 'a');
ASSERT VALUES `OUTPUT` (F2, KSQL_COL_0) VALUES ('a', 2);
ASSERT VALUES `OUTPUT` (F2, KSQL_COL_0) VALUES ('b', 2);
ASSERT VALUES `OUTPUT` (F2) VALUES ('a');
ASSERT VALUES `OUTPUT` (F2, KSQL_COL_0) VALUES ('a', 4);

--@test: group-by - with constant having (stream-table)
CREATE STREAM TEST (K STRING KEY, f1 INT, f2 VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f2, SUM(f1) FROM TEST GROUP BY f2 HAVING f2='test';
INSERT INTO `TEST` (K, F1, F2) VALUES ('0', 1, 'a');
INSERT INTO `TEST` (K, F1, F2) VALUES ('0', 2, 'b');
INSERT INTO `TEST` (K, F1, F2) VALUES ('0', 2, 'test');
INSERT INTO `TEST` (K, F1, F2) VALUES ('0', 2, 'b');
INSERT INTO `TEST` (K, F1, F2) VALUES ('0', 3, 'test');
ASSERT VALUES `OUTPUT` (F2, KSQL_COL_0) VALUES ('test', 2);
ASSERT VALUES `OUTPUT` (F2, KSQL_COL_0) VALUES ('test', 5);

--@test: group-by - with constants in the projection (stream->table)
CREATE STREAM TEST (ID INT KEY, f1 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f1, 'some constant' as f3, COUNT(f1) FROM TEST GROUP BY f1;
INSERT INTO `TEST` (ID, F1) VALUES (1, 1);
INSERT INTO `TEST` (ID, F1) VALUES (2, 2);
INSERT INTO `TEST` (ID, F1) VALUES (1, 1);
INSERT INTO `TEST` (ID, F1) VALUES (2, 2);
INSERT INTO `TEST` (ID, F1) VALUES (3, 3);
ASSERT VALUES `OUTPUT` (F1, F3, KSQL_COL_0) VALUES (1, 'some constant', 1);
ASSERT VALUES `OUTPUT` (F1, F3, KSQL_COL_0) VALUES (2, 'some constant', 1);
ASSERT VALUES `OUTPUT` (F1, F3, KSQL_COL_0) VALUES (1, 'some constant', 2);
ASSERT VALUES `OUTPUT` (F1, F3, KSQL_COL_0) VALUES (2, 'some constant', 2);
ASSERT VALUES `OUTPUT` (F1, F3, KSQL_COL_0) VALUES (3, 'some constant', 1);

--@test: group-by - missing matching projection field (stream->table)
CREATE STREAM TEST (K STRING KEY, data VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT data, COUNT(*) FROM TEST GROUP BY data;
INSERT INTO `TEST` (DATA, ROWTIME) VALUES ('d1', 1);
INSERT INTO `TEST` (DATA, ROWTIME) VALUES ('d2', 2);
INSERT INTO `TEST` (DATA, ROWTIME) VALUES ('d1', 3);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0, ROWTIME) VALUES ('d1', 1, 1);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0, ROWTIME) VALUES ('d2', 1, 2);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0, ROWTIME) VALUES ('d1', 2, 3);

--@test: group-by - missing matching projection field (table->table)
CREATE TABLE TEST (K STRING PRIMARY KEY, f1 INT, f2 VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT F2, COUNT(*) FROM TEST GROUP BY f2;
INSERT INTO `TEST` (K, F1, F2) VALUES ('1', 1, 'a');
INSERT INTO `TEST` (K, F1, F2) VALUES ('2', 2, 'b');
INSERT INTO `TEST` (K, F1, F2) VALUES ('1', 1, 'b');
INSERT INTO `TEST` (K) VALUES ('2');
INSERT INTO `TEST` (K, F1, F2) VALUES ('1', 1, 'a');
ASSERT VALUES `OUTPUT` (F2, KSQL_COL_0) VALUES ('a', 1);
ASSERT VALUES `OUTPUT` (F2, KSQL_COL_0) VALUES ('b', 1);
ASSERT VALUES `OUTPUT` (F2, KSQL_COL_0) VALUES ('a', 0);
ASSERT VALUES `OUTPUT` (F2, KSQL_COL_0) VALUES ('b', 2);
ASSERT VALUES `OUTPUT` (F2, KSQL_COL_0) VALUES ('b', 1);
ASSERT VALUES `OUTPUT` (F2, KSQL_COL_0) VALUES ('b', 0);
ASSERT VALUES `OUTPUT` (F2, KSQL_COL_0) VALUES ('a', 1);

--@test: group-by - duplicate fields (stream->table)
CREATE STREAM TEST (K STRING KEY, data VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT DATA, COUNT(1), COUNT(*), AS_VALUE(DATA) AS COPY FROM TEST GROUP BY data;
INSERT INTO `TEST` (DATA, ROWTIME) VALUES ('d1', 1);
INSERT INTO `TEST` (DATA, ROWTIME) VALUES ('d2', 2);
INSERT INTO `TEST` (DATA, ROWTIME) VALUES ('d1', 3);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0, KSQL_COL_1, COPY, ROWTIME) VALUES ('d1', 1, 1, 'd1', 1);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0, KSQL_COL_1, COPY, ROWTIME) VALUES ('d2', 1, 1, 'd2', 2);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0, KSQL_COL_1, COPY, ROWTIME) VALUES ('d1', 2, 2, 'd1', 3);

--@test: group-by - duplicate udafs (stream->table)
CREATE STREAM TEST (K STRING KEY, data VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT DATA, COUNT(1), COUNT(1) FROM TEST GROUP BY data;
INSERT INTO `TEST` (DATA, ROWTIME) VALUES ('d1', 1);
INSERT INTO `TEST` (DATA, ROWTIME) VALUES ('d2', 2);
INSERT INTO `TEST` (DATA, ROWTIME) VALUES ('d1', 3);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0, KSQL_COL_1, ROWTIME) VALUES ('d1', 1, 1, 1);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0, KSQL_COL_1, ROWTIME) VALUES ('d2', 1, 1, 2);
ASSERT VALUES `OUTPUT` (DATA, KSQL_COL_0, KSQL_COL_1, ROWTIME) VALUES ('d1', 2, 2, 3);

--@test: group-by - with non-aggregate projection field not in group by (stream->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: D1
CREATE STREAM TEST (K STRING KEY, d1 VARCHAR, d2 VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT d1, COUNT(*) FROM TEST GROUP BY d2;
--@test: group-by - with non-aggregate projection field not in group by (table->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: D1
CREATE TABLE TEST (K STRING PRIMARY KEY, d1 VARCHAR, d2 VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT d1, COUNT(*) FROM TEST GROUP BY d2;
--@test: group-by - aggregate function (stream->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: GROUP BY does not support aggregate functions: SUM is an aggregate function.
CREATE STREAM TEST (K STRING KEY, d1 VARCHAR, d2 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT COUNT(*) FROM TEST GROUP BY SUM(d2);
--@test: group-by - aggregate function nested in arithmetic (stream->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: GROUP BY does not support aggregate functions: SUM is an aggregate function.
CREATE STREAM TEST (K STRING KEY, d1 VARCHAR, d2 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT COUNT(*) FROM TEST GROUP BY 1 + SUM(d2);
--@test: group-by - aggregate function nested in UDF (stream->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: GROUP BY does not support aggregate functions: SUM is an aggregate function.
CREATE STREAM TEST (K STRING KEY, d1 VARCHAR, d2 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT COUNT(*) FROM TEST GROUP BY SUBSTRING(d1, SUM(d2), 1);
--@test: group-by - without aggregate functions (stream->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: GROUP BY requires aggregate functions in either the SELECT or HAVING clause.
CREATE STREAM TEST (K STRING KEY, d1 VARCHAR, d2 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT SUBSTRING(d1, 1, 2) FROM TEST GROUP BY d2;
--@test: group-by - without group-by (stream->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Non-aggregate SELECT expression(s) not part of GROUP BY: D1
CREATE STREAM TEST (K STRING KEY, d1 VARCHAR, d2 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT d1, COUNT() FROM TEST;
--@test: group-by - UDAF nested in UDF in select expression (stream->table)
CREATE STREAM TEST (K STRING KEY, d1 VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT D1, SUBSTRING('Mr Bugalicious', CAST(COUNT(*) AS INT), 1) FROM TEST GROUP BY d1;
INSERT INTO `TEST` (D1) VALUES ('x');
INSERT INTO `TEST` (D1) VALUES ('xxx');
INSERT INTO `TEST` (D1) VALUES ('y');
INSERT INTO `TEST` (D1) VALUES ('x');
INSERT INTO `TEST` (D1) VALUES ('xxx');
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('x', 'M');
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('xxx', 'M');
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('y', 'M');
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('x', 'r');
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('xxx', 'r');

--@test: group-by - UDAF nested in UDF in select expression (table->table)
CREATE TABLE TEST (K STRING PRIMARY KEY, d0 INT, d1 VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT d1, SUBSTRING('Mr Bugalicious', CAST(COUNT(*) AS INT), 1) FROM TEST GROUP BY d1;
INSERT INTO `TEST` (K, D0, D1) VALUES ('0', 0, 'x');
INSERT INTO `TEST` (K, D0, D1) VALUES ('1', 1, 'x');
INSERT INTO `TEST` (K, D0, D1) VALUES ('2', 2, 'xxx');
INSERT INTO `TEST` (K, D0, D1) VALUES ('3', 3, 'xxx');
INSERT INTO `TEST` (K) VALUES ('1');
INSERT INTO `TEST` (K, D0, D1) VALUES ('2', 2, 'yy');
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('x', 'M');
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('x', 'r');
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('xxx', 'M');
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('xxx', 'r');
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('x', 'M');
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('xxx', 'M');
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('yy', 'M');

--@test: group-by - UDF nested in UDAF in select expression (stream->table)
CREATE STREAM TEST (K STRING KEY, d1 VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT d1, SUM(LEN(d1)) FROM TEST GROUP BY d1;
INSERT INTO `TEST` (D1) VALUES ('x');
INSERT INTO `TEST` (D1) VALUES ('xxx');
INSERT INTO `TEST` (D1) VALUES ('y');
INSERT INTO `TEST` (D1) VALUES ('x');
INSERT INTO `TEST` (D1) VALUES ('xxx');
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('x', 1);
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('xxx', 3);
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('y', 1);
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('x', 2);
ASSERT VALUES `OUTPUT` (D1, KSQL_COL_0) VALUES ('xxx', 6);

--@test: group-by - UDAF nested in UDAF in select expression (stream->table)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Aggregate functions can not be nested: SUM(COUNT())
CREATE STREAM TEST (K STRING KEY, d1 VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT SUM(COUNT()) FROM TEST GROUP BY d1;
--@test: group-by - should exclude any stream row whose single GROUP BY expression resolves to NULL
CREATE STREAM TEST (K STRING KEY, str STRING, pos INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT SUBSTRING(str, pos), COUNT() FROM TEST GROUP BY SUBSTRING(str, pos);
INSERT INTO `TEST` (STR, POS) VALUES ('xx', 1);
INSERT INTO `TEST` (STR, POS) VALUES ('x', NULL);
INSERT INTO `TEST` (STR, POS) VALUES ('xx', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('xx', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('xx', 2);

--@test: group-by - should exclude any table row whose single GROUP BY expression resolves to NULL
CREATE TABLE TEST (K STRING PRIMARY KEY, str STRING, pos INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT SUBSTRING(str, pos), COUNT() FROM TEST GROUP BY SUBSTRING(str, pos);
INSERT INTO `TEST` (K, STR, POS) VALUES ('1', 'xx', 1);
INSERT INTO `TEST` (K, STR, POS) VALUES ('2', 'x', NULL);
INSERT INTO `TEST` (K, STR, POS) VALUES ('3', 'xx', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('xx', 1);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('xx', 2);

--@test: group-by - should exclude any stream row whose single GROUP BY expression throws
CREATE STREAM TEST (K STRING KEY, id INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT BAD_UDF(id), COUNT() FROM TEST GROUP BY BAD_UDF(id);
INSERT INTO `TEST` (ID) VALUES (1);


--@test: group-by - should exclude any table row whose single GROUP BY expression throws
CREATE TABLE TEST (K STRING PRIMARY KEY, id INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT BAD_UDF(id), COUNT() FROM TEST GROUP BY BAD_UDF(id);
INSERT INTO `TEST` (K, ID) VALUES ('2', 1);


--@test: group-by - by non-STRING key
CREATE STREAM INPUT (K STRING KEY, f0 INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT f0, COUNT(1) FROM INPUT GROUP BY f0;
INSERT INTO `INPUT` (F0) VALUES (2);
INSERT INTO `INPUT` (F0) VALUES (3);
INSERT INTO `INPUT` (F0) VALUES (2);
ASSERT VALUES `OUTPUT` (F0, KSQL_COL_0) VALUES (2, 1);
ASSERT VALUES `OUTPUT` (F0, KSQL_COL_0) VALUES (3, 1);
ASSERT VALUES `OUTPUT` (F0, KSQL_COL_0) VALUES (2, 2);
ASSERT table OUTPUT (F0 INTEGER PRIMARY KEY, KSQL_COL_0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - should handled quoted key and value
CREATE STREAM INPUT (`Key` STRING KEY, IGNORED INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT `Key`, COUNT(1) AS `Value` FROM INPUT GROUP BY `Key`;
INSERT INTO `INPUT` (Key, ROWTIME) VALUES ('11', 12345);
INSERT INTO `INPUT` (Key, ROWTIME) VALUES ('10', 12365);
INSERT INTO `INPUT` (Key, ROWTIME) VALUES ('11', 12375);
ASSERT VALUES `OUTPUT` (Key, Value) VALUES ('11', 1);
ASSERT VALUES `OUTPUT` (Key, Value) VALUES ('10', 1);
ASSERT VALUES `OUTPUT` (Key, Value) VALUES ('11', 2);
ASSERT table OUTPUT (`Key` STRING PRIMARY KEY, `Value` BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: group-by - on join
CREATE TABLE t1 (ID BIGINT PRIMARY KEY, TOTAL integer) WITH (kafka_topic='T1', value_format='AVRO');
CREATE TABLE t2 (ID BIGINT PRIMARY KEY, TOTAL integer) WITH (kafka_topic='T2', value_format='AVRO');
CREATE TABLE OUTPUT AS SELECT t1.ID, SUM(t1.total + CASE WHEN t2.total IS NULL THEN 0 ELSE t2.total END) as SUM FROM T1 LEFT JOIN T2 ON (t1.ID = t2.ID) GROUP BY t1.ID HAVING COUNT(1) > 0;
INSERT INTO `T1` (ID, total) VALUES (0, 100);
INSERT INTO `T1` (ID, total) VALUES (1, 101);
INSERT INTO `T2` (ID, total) VALUES (0, 5);
INSERT INTO `T2` (ID, total) VALUES (1, 10);
INSERT INTO `T2` (ID, total) VALUES (0, 20);
INSERT INTO `T2` (ID) VALUES (0);
ASSERT VALUES `OUTPUT` (T1_ID, SUM) VALUES (0, 100);
ASSERT VALUES `OUTPUT` (T1_ID, SUM) VALUES (1, 101);
ASSERT VALUES `OUTPUT` (T1_ID) VALUES (0);
ASSERT VALUES `OUTPUT` (T1_ID, SUM) VALUES (0, 105);
ASSERT VALUES `OUTPUT` (T1_ID) VALUES (1);
ASSERT VALUES `OUTPUT` (T1_ID, SUM) VALUES (1, 111);
ASSERT VALUES `OUTPUT` (T1_ID) VALUES (0);
ASSERT VALUES `OUTPUT` (T1_ID, SUM) VALUES (0, 120);
ASSERT VALUES `OUTPUT` (T1_ID) VALUES (0);
ASSERT VALUES `OUTPUT` (T1_ID, SUM) VALUES (0, 100);

--@test: group-by - windowed join
CREATE TABLE A (id varchar primary key, regionid varchar) WITH (kafka_topic='a', value_format='json');
CREATE STREAM B (id varchar) WITH (kafka_topic='b', value_format='json');
CREATE TABLE test AS SELECT a.id, COUNT(*) as count FROM B LEFT JOIN A ON a.id = b.id WINDOW TUMBLING (SIZE 1 MINUTE) GROUP BY a.id HAVING COUNT(*) > 2;
INSERT INTO `A` (ID, id, regionid) VALUES ('1', '1', 'one');
INSERT INTO `B` (Id) VALUES ('1');
INSERT INTO `B` (Id) VALUES ('1');
INSERT INTO `B` (Id) VALUES ('1');
ASSERT VALUES `TEST` (A_ID, COUNT, WINDOWSTART, WINDOWEND) VALUES ('1', 3, 0, 60000);

--@test: group-by - windowed join with window bounds
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: SELECT column 'WINDOWSTART' cannot be resolved.
CREATE STREAM A (ID VARCHAR, col1 VARCHAR) WITH (kafka_topic='a', value_format='JSON');
CREATE TABLE B (ID VARCHAR PRIMARY KEY, col1 VARCHAR) WITH (kafka_topic='b', value_format='JSON');
CREATE TABLE C AS SELECT A.ID, COUNT(*), WINDOWSTART as WSTART, WINDOWEND AS WEND FROM A JOIN B on A.ID = B.ID WINDOW TUMBLING (SIZE 10 MILLISECONDS) GROUP BY a.ID;
--@test: group-by - zero non-agg columns (stream)
CREATE STREAM INPUT (VALUE INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT as SELECT 1 as k, count(1) AS ID FROM INPUT group by 1;
INSERT INTO `INPUT` (VALUE) VALUES (0);
INSERT INTO `INPUT` (VALUE) VALUES (0);
INSERT INTO `INPUT` (VALUE) VALUES (0);
ASSERT VALUES `OUTPUT` (K, ID) VALUES (1, 1);
ASSERT VALUES `OUTPUT` (K, ID) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (K, ID) VALUES (1, 3);

--@test: group-by - zero non-agg columns (windowed stream)
CREATE STREAM INPUT (VALUE INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT as SELECT 1 as k, count(1) AS ID FROM INPUT WINDOW TUMBLING (SIZE 1 SECOND) group by 1;
INSERT INTO `INPUT` (VALUE) VALUES (0);
INSERT INTO `INPUT` (VALUE) VALUES (0);
INSERT INTO `INPUT` (VALUE) VALUES (0);
ASSERT VALUES `OUTPUT` (K, ID, WINDOWSTART, WINDOWEND) VALUES (1, 1, 0, 1000);
ASSERT VALUES `OUTPUT` (K, ID, WINDOWSTART, WINDOWEND) VALUES (1, 2, 0, 1000);
ASSERT VALUES `OUTPUT` (K, ID, WINDOWSTART, WINDOWEND) VALUES (1, 3, 0, 1000);

--@test: group-by - zero non-agg columns (table)
CREATE TABLE INPUT (ID INT PRIMARY KEY, VALUE INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT as SELECT 1 as k, count(1) AS ID FROM INPUT group by 1;
INSERT INTO `INPUT` (ID, VALUE) VALUES (10, 0);
INSERT INTO `INPUT` (ID, VALUE) VALUES (1666, 0);
INSERT INTO `INPUT` (ID, VALUE) VALUES (98, 0);
ASSERT VALUES `OUTPUT` (K, ID) VALUES (1, 1);
ASSERT VALUES `OUTPUT` (K, ID) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (K, ID) VALUES (1, 3);

--@test: group-by - windowed aggregate with struct key - JSON
CREATE STREAM INPUT (ID STRUCT<F1 INT, F2 INT> KEY, VAL INT) WITH (kafka_topic='test_topic', format='JSON');
CREATE TABLE OUTPUT as SELECT ID, count(1) AS count FROM INPUT WINDOW TUMBLING (SIZE 1 SECOND) group by ID;
INSERT INTO `INPUT` (ID, VAL) VALUES (STRUCT(F1:=1, F2:=1), 0);
INSERT INTO `INPUT` (ID, VAL) VALUES (STRUCT(F1:=1, F2:=1), 0);
INSERT INTO `INPUT` (ID, VAL) VALUES (STRUCT(F1:=1, F2:=1), 0);
ASSERT VALUES `OUTPUT` (ID, COUNT, WINDOWSTART, WINDOWEND) VALUES (STRUCT(F1:=1, F2:=1), 1, 0, 1000);
ASSERT VALUES `OUTPUT` (ID, COUNT, WINDOWSTART, WINDOWEND) VALUES (STRUCT(F1:=1, F2:=1), 2, 0, 1000);
ASSERT VALUES `OUTPUT` (ID, COUNT, WINDOWSTART, WINDOWEND) VALUES (STRUCT(F1:=1, F2:=1), 3, 0, 1000);

--@test: group-by - windowed aggregate with struct key - AVRO
CREATE STREAM INPUT (ID STRUCT<F1 INT, F2 INT> KEY, VAL INT) WITH (kafka_topic='test_topic', format='AVRO');
CREATE TABLE OUTPUT as SELECT ID, count(1) AS count FROM INPUT WINDOW TUMBLING (SIZE 1 SECOND) group by ID;
INSERT INTO `INPUT` (ID, VAL) VALUES (STRUCT(F1:=1, F2:=1), 0);
INSERT INTO `INPUT` (ID, VAL) VALUES (STRUCT(F1:=1, F2:=1), 0);
INSERT INTO `INPUT` (ID, VAL) VALUES (STRUCT(F1:=1, F2:=1), 0);
ASSERT VALUES `OUTPUT` (ID, COUNT, WINDOWSTART, WINDOWEND) VALUES (STRUCT(F1:=1, F2:=1), 1, 0, 1000);
ASSERT VALUES `OUTPUT` (ID, COUNT, WINDOWSTART, WINDOWEND) VALUES (STRUCT(F1:=1, F2:=1), 2, 0, 1000);
ASSERT VALUES `OUTPUT` (ID, COUNT, WINDOWSTART, WINDOWEND) VALUES (STRUCT(F1:=1, F2:=1), 3, 0, 1000);

--@test: group-by - windowed aggregate with field within struct key
CREATE STREAM INPUT (ID STRUCT<F1 INT, F2 INT> KEY, VAL INT) WITH (kafka_topic='test_topic', format='JSON');
CREATE TABLE OUTPUT as SELECT ID->F1, count(1) AS count FROM INPUT WINDOW TUMBLING (SIZE 1 SECOND) group by ID->F1;
INSERT INTO `INPUT` (ID, VAL) VALUES (STRUCT(F1:=1, F2:=1), 0);
INSERT INTO `INPUT` (ID, VAL) VALUES (STRUCT(F1:=1, F2:=1), 0);
INSERT INTO `INPUT` (ID, VAL) VALUES (STRUCT(F1:=1, F2:=1), 0);
ASSERT VALUES `OUTPUT` (F1, COUNT, WINDOWSTART, WINDOWEND) VALUES (1, 1, 0, 1000);
ASSERT VALUES `OUTPUT` (F1, COUNT, WINDOWSTART, WINDOWEND) VALUES (1, 2, 0, 1000);
ASSERT VALUES `OUTPUT` (F1, COUNT, WINDOWSTART, WINDOWEND) VALUES (1, 3, 0, 1000);

--@test: group-by - non-KAFKA key format
CREATE STREAM TEST (ID INT KEY, VAL BOOLEAN) WITH (kafka_topic='test_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT VAL, COUNT() AS COUNT FROM TEST GROUP BY VAL;
INSERT INTO `TEST` (ID, VAL) VALUES (0, true);
INSERT INTO `TEST` (ID, VAL) VALUES (1, false);
INSERT INTO `TEST` (ID, VAL) VALUES (2, true);
ASSERT VALUES `OUTPUT` (VAL, COUNT) VALUES (true, 1);
ASSERT VALUES `OUTPUT` (VAL, COUNT) VALUES (false, 1);
ASSERT VALUES `OUTPUT` (VAL, COUNT) VALUES (true, 2);

--@test: group-by - JSON group by array
CREATE STREAM TEST (ID INT KEY, A INT, B INT) WITH (kafka_topic='test_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT ARRAY[a, b] AS ROWKEY, COUNT() AS COUNT FROM TEST GROUP BY ARRAY[A, B];
INSERT INTO `TEST` (ID, A, B) VALUES (0, 1, 1);
INSERT INTO `TEST` (ID, A, B) VALUES (1, 2, 1);
INSERT INTO `TEST` (ID, A, B) VALUES (2, 1, 1);
ASSERT VALUES `OUTPUT` (ROWKEY, COUNT) VALUES (ARRAY[1, 1], 1);
ASSERT VALUES `OUTPUT` (ROWKEY, COUNT) VALUES (ARRAY[2, 1], 1);
ASSERT VALUES `OUTPUT` (ROWKEY, COUNT) VALUES (ARRAY[1, 1], 2);

--@test: group-by - JSON group by struct
CREATE STREAM TEST (ID INT KEY, A INT, B INT) WITH (kafka_topic='test_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT STRUCT(a:=A, b:=B) AS ROWKEY, COUNT() AS COUNT FROM TEST GROUP BY STRUCT(a:=A, b:=B);
INSERT INTO `TEST` (ID, A, B) VALUES (0, 1, 1);
INSERT INTO `TEST` (ID, A, B) VALUES (1, 2, 1);
INSERT INTO `TEST` (ID, A, B) VALUES (2, 1, 1);
ASSERT VALUES `OUTPUT` (ROWKEY, COUNT) VALUES (STRUCT(A:=1, B:=1), 1);
ASSERT VALUES `OUTPUT` (ROWKEY, COUNT) VALUES (STRUCT(A:=2, B:=1), 1);
ASSERT VALUES `OUTPUT` (ROWKEY, COUNT) VALUES (STRUCT(A:=1, B:=1), 2);

--@test: group-by - JSON group by struct convert to incompatible key format
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'DELIMITED' format does not support type 'STRUCT', column: `ROWKEY`
CREATE STREAM TEST (ID INT KEY, A INT, B INT) WITH (kafka_topic='test_topic', format='JSON');
CREATE TABLE OUTPUT WITH (key_format='DELIMITED') AS SELECT STRUCT(a:=A, b:=B) AS ROWKEY, COUNT() AS COUNT FROM TEST GROUP BY STRUCT(a:=A, b:=B);
--@test: group-by - Struct key used in aggregate expression
CREATE STREAM TEST (ID STRUCT<F1 INT> KEY, VAL INT) WITH (kafka_topic='test_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT ID, SUM(ID->F1) AS sum FROM TEST GROUP BY ID;
INSERT INTO `TEST` (ID, VAL) VALUES (STRUCT(F1:=1), 1);
INSERT INTO `TEST` (ID, VAL) VALUES (STRUCT(F1:=1), 2);
ASSERT VALUES `OUTPUT` (ID, SUM) VALUES (STRUCT(F1:=1), 1);
ASSERT VALUES `OUTPUT` (ID, SUM) VALUES (STRUCT(F1:=1), 2);

