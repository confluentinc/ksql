--@test: create-struct - basic struct creation
CREATE STREAM INPUT (ID STRING KEY, col1 VARCHAR, col2 ARRAY<VARCHAR>) WITH (kafka_topic='test', value_format='JSON');
CREATE STREAM BIG_STRUCT AS SELECT ID, STRUCT(F1 := COL1, F2 := COL2, F3 := SUBSTRING(col1, 2)) AS s FROM INPUT;
INSERT INTO `INPUT` (col1, col2) VALUES ('foo', ARRAY['bar']);
ASSERT VALUES `BIG_STRUCT` (S) VALUES (STRUCT(F1:='foo', F2:=ARRAY['bar'], F3:='oo'));
ASSERT stream BIG_STRUCT (ID STRING KEY, S STRUCT<F1 STRING, F2 ARRAY<STRING>, F3 STRING>) WITH (KAFKA_TOPIC='BIG_STRUCT');

--@test: create-struct - nested struct creation
CREATE STREAM INPUT (ID STRING KEY, col1 VARCHAR) WITH (kafka_topic='test', value_format='JSON');
CREATE STREAM BIG_STRUCT AS SELECT ID, STRUCT(f1 := STRUCT(c1 := col1)) AS s FROM INPUT;
INSERT INTO `INPUT` (col1) VALUES ('foo');
ASSERT VALUES `BIG_STRUCT` (S) VALUES (STRUCT(F1:=STRUCT(C1:='foo')));
ASSERT stream BIG_STRUCT (ID STRING KEY, S STRUCT<F1 STRUCT<C1 STRING>>) WITH (KAFKA_TOPIC='BIG_STRUCT');

--@test: create-struct - quoted identifiers
CREATE STREAM INPUT (ID STRING KEY, col1 VARCHAR) WITH (kafka_topic='test', value_format='JSON');
CREATE STREAM BIG_STRUCT AS SELECT ID, STRUCT(FOO := col1, `foo` := col1) AS s FROM INPUT;
INSERT INTO `INPUT` (col1) VALUES ('foo');
ASSERT VALUES `BIG_STRUCT` (S) VALUES (STRUCT(FOO:='foo', `foo`:='foo'));
ASSERT stream BIG_STRUCT (ID STRING KEY, S STRUCT<FOO VARCHAR, `foo` VARCHAR>) WITH (KAFKA_TOPIC='BIG_STRUCT');

--@test: create-struct - empty struct creation
CREATE STREAM INPUT (ID STRING KEY, col1 VARCHAR) WITH (kafka_topic='test', value_format='JSON');
CREATE STREAM BIG_STRUCT AS SELECT ID, STRUCT() AS s FROM INPUT;
INSERT INTO `INPUT` (col1) VALUES ('foo');
ASSERT VALUES `BIG_STRUCT` (S) VALUES (STRUCT());
ASSERT stream BIG_STRUCT (ID STRING KEY, S STRUCT< >) WITH (KAFKA_TOPIC='BIG_STRUCT');

--@test: create-struct - duplicate fields
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Duplicate field names found in STRUCT
CREATE STREAM INPUT (ID STRING KEY, col1 VARCHAR) WITH (kafka_topic='test', value_format='JSON');
CREATE STREAM BIG_STRUCT AS SELECT ID, STRUCT(foo := col1, foo := col1) AS s FROM INPUT;
--@test: create-struct - duplicate structs in array
CREATE STREAM INPUT (ignored VARCHAR) WITH (kafka_topic='test',value_format='json',partitions=1);
CREATE STREAM OUTPUT AS SELECT array[struct(a:=123),struct(a:=123)] from INPUT emit changes;
INSERT INTO `INPUT` (ignored) VALUES ('hello world');
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (ARRAY[STRUCT(A:=123), STRUCT(A:=123)]);

--@test: create-struct - cast null values
CREATE STREAM INPUT (ID STRING KEY, ignored STRING) WITH (kafka_topic='test', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, STRUCT(F1 := CAST(NULL AS INT)) AS s FROM INPUT;
INSERT INTO `INPUT` (ignored) VALUES ('yay');
ASSERT VALUES `OUTPUT` (S) VALUES (STRUCT(F1:=NULL));
ASSERT stream OUTPUT (ID STRING KEY, S STRUCT<F1 INT>) WITH (KAFKA_TOPIC='OUTPUT');

--@test: create-struct - in aggregate
CREATE STREAM INPUT (ID INT KEY, VAL INT) WITH (kafka_topic='test', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT ID, STRUCT(Vals := COLLECT_LIST(VAL)) AS body FROM INPUT GROUP BY ID;
INSERT INTO `INPUT` (ID, VAL) VALUES (1, 1);
INSERT INTO `INPUT` (ID, VAL) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (ID, BODY) VALUES (1, STRUCT(VALS:=ARRAY[1]));
ASSERT VALUES `OUTPUT` (ID, BODY) VALUES (1, STRUCT(VALS:=ARRAY[1, 2]));
ASSERT table OUTPUT (ID INT PRIMARY KEY, BODY STRUCT<VALS ARRAY<INT>>) WITH (KAFKA_TOPIC='OUTPUT');

