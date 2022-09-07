--@test: udf - nested
CREATE STREAM INPUT (K STRING KEY, text STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, CONCAT(EXTRACTJSONFIELD(text,'$.name'),CONCAT('-',EXTRACTJSONFIELD(text,'$.value'))) from INPUT;
INSERT INTO `INPUT` (text) VALUES ('{"name":"fred","value":1}');
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES ('fred-1');
ASSERT stream OUTPUT (K STRING KEY, KSQL_COL_0 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: udf - null args
CREATE STREAM INPUT (K STRING KEY, ID BIGINT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT K, test_udf(ID, NULL), test_udf(ID, ID, NULL) from INPUT;
INSERT INTO `INPUT` (ID) VALUES (0);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('doStuffLongString', 'doStuffLongLongString');
ASSERT stream OUTPUT (K STRING KEY, KSQL_COL_0 STRING, KSQL_COL_1 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: udf - var args
CREATE STREAM INPUT (K STRING KEY, col0 BIGINT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT K, test_udf(col0, col0, col0, col0, col0) from INPUT;
INSERT INTO `INPUT` (COL0) VALUES (0);
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES ('doStuffLongVarargs');
ASSERT stream OUTPUT (`K` STRING KEY, `KSQL_COL_0` STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: udf - struct args
CREATE STREAM INPUT (K STRING KEY, col0 STRUCT<A STRING>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT k, test_udf(col0) from INPUT;
INSERT INTO `INPUT` (col0) VALUES (STRUCT(A:='expect-result'));
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES ('expect-result');
ASSERT stream OUTPUT (K STRING KEY, KSQL_COL_0 STRING) WITH (KAFKA_TOPIC='OUTPUT');

