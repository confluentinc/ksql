--@test: delimited - BOOLEAN - key
CREATE STREAM INPUT (K BOOLEAN KEY, V INT) WITH (kafka_topic='input_topic', format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, V) VALUES (true, 0);
INSERT INTO `INPUT` (K, V) VALUES (true, 0);
INSERT INTO `INPUT` (K, V) VALUES (false, 0);
INSERT INTO `INPUT` (V) VALUES (0);
ASSERT VALUES `OUTPUT` (K, V) VALUES (true, 0);
ASSERT VALUES `OUTPUT` (K, V) VALUES (true, 0);
ASSERT VALUES `OUTPUT` (K, V) VALUES (false, 0);
ASSERT VALUES `OUTPUT` (V) VALUES (0);

--@test: delimited - INT - key
CREATE STREAM INPUT (K INT KEY, V INT) WITH (kafka_topic='input_topic', format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, V) VALUES (33, 0);
INSERT INTO `INPUT` (K, V) VALUES (22, 0);
INSERT INTO `INPUT` (V) VALUES (0);
ASSERT VALUES `OUTPUT` (K, V) VALUES (33, 0);
ASSERT VALUES `OUTPUT` (K, V) VALUES (22, 0);
ASSERT VALUES `OUTPUT` (V) VALUES (0);

--@test: delimited - BIGINT - key
CREATE STREAM INPUT (K BIGINT KEY, V INT) WITH (kafka_topic='input_topic', format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, V) VALUES (998877665544332211, 0);
INSERT INTO `INPUT` (K, V) VALUES (10, 0);
INSERT INTO `INPUT` (V) VALUES (0);
ASSERT VALUES `OUTPUT` (K, V) VALUES (998877665544332211, 0);
ASSERT VALUES `OUTPUT` (K, V) VALUES (10, 0);
ASSERT VALUES `OUTPUT` (V) VALUES (0);

--@test: delimited - DOUBLE - key
CREATE STREAM INPUT (K DOUBLE KEY, V INT) WITH (kafka_topic='input_topic', format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, V) VALUES (654.321, 0);
INSERT INTO `INPUT` (K, V) VALUES (123.456, 0);
INSERT INTO `INPUT` (V) VALUES (0);
ASSERT VALUES `OUTPUT` (K, V) VALUES (654.321, 0);
ASSERT VALUES `OUTPUT` (K, V) VALUES (123.456, 0);
ASSERT VALUES `OUTPUT` (V) VALUES (0);

--@test: delimited - STRING - key
CREATE STREAM INPUT (K STRING KEY, V INT) WITH (kafka_topic='input_topic', format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, V) VALUES ('Hey', 0);
INSERT INTO `INPUT` (K, V) VALUES ('"You"', 0);
INSERT INTO `INPUT` (V) VALUES (0);
ASSERT VALUES `OUTPUT` (K, V) VALUES ('Hey', 0);
ASSERT VALUES `OUTPUT` (K, V) VALUES ('You', 0);
ASSERT VALUES `OUTPUT` (V) VALUES (0);

--@test: delimited - DECIMAL - key
CREATE STREAM INPUT (K DECIMAL(6,4) KEY, V INT) WITH (kafka_topic='input_topic', format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, V) VALUES (12.3650, 0);
INSERT INTO `INPUT` (K, V) VALUES (12.0, 0);
INSERT INTO `INPUT` (V) VALUES (0);
ASSERT VALUES `OUTPUT` (K, V) VALUES (12.3650, 0);
ASSERT VALUES `OUTPUT` (K, V) VALUES (12.0000, 0);
ASSERT VALUES `OUTPUT` (V) VALUES (0);

--@test: delimited - multi-column key
CREATE STREAM INPUT (K1 STRING KEY, K2 STRING KEY, V INT) WITH (kafka_topic='input_topic', format='DELIMITED');
CREATE STREAM OUTPUT WITH (key_format='JSON') AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K1, K2, V) VALUES ('foo', 'bar', 0);
INSERT INTO `INPUT` (K1, K2, V) VALUES ('"foo"', '"bar"', 0);
INSERT INTO `INPUT` (V) VALUES (0);
ASSERT VALUES `OUTPUT` (K1, K2, V) VALUES ('foo', 'bar', 0);
ASSERT VALUES `OUTPUT` (K1, K2, V) VALUES ('foo', 'bar', 0);
ASSERT VALUES `OUTPUT` (V) VALUES (0);

--@test: delimited - multi-column key with different kv delimiters
CREATE STREAM INPUT (K1 STRING KEY, K2 STRING KEY, V INT, V2 INT) WITH (kafka_topic='input_topic', format='DELIMITED', key_delimiter=';', value_delimiter='-');
CREATE STREAM OUTPUT WITH (key_format='JSON') AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K1, K2, V, V2) VALUES ('foo', 'bar;baz', 0-1, NULL);
ASSERT VALUES `OUTPUT` (K1, K2, V, V2) VALUES ('foo,bar', 'baz', 0-1, NULL);

--@test: delimited - ARRAY - key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Key format does not support schema.
CREATE STREAM INPUT (K ARRAY<DOUBLE> KEY, V INT) WITH (kafka_topic='input_topic', format='DELIMITED');
--@test: delimited - MAP - key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Map keys, including types that contain maps, are not supported as they may lead to unexpected behavior due to inconsistent serialization. Key column name: `K`. Column type: MAP<STRING, DOUBLE>. See https://github.com/confluentinc/ksql/issues/6621 for more.
CREATE STREAM INPUT (K MAP<STRING, DOUBLE> KEY, V INT) WITH (kafka_topic='input_topic', format='DELIMITED');
--@test: delimited - STRUCT - key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Key format does not support schema.
CREATE STREAM INPUT (K STRUCT<F DOUBLE> KEY, V INT) WITH (kafka_topic='input_topic', format='DELIMITED');
--@test: delimited - ARRAY - C* - key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'DELIMITED' format does not support type 'ARRAY'
CREATE STREAM INPUT (K ARRAY<INT> KEY, foo INT) WITH (kafka_topic='input_topic', format='DELIMITED');
--@test: delimited - ARRAY - C*AS - key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'DELIMITED' format does not support type 'ARRAY'
CREATE STREAM INPUT (v INT) WITH (kafka_topic='input_topic', format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT PARTITION BY ARRAY[v];
--@test: delimited - MAP - C* - key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Map keys, including types that contain maps, are not supported as they may lead to unexpected behavior due to inconsistent serialization. Key column name: `K`. Column type: MAP<INTEGER, DOUBLE>. See https://github.com/confluentinc/ksql/issues/6621 for more.
CREATE STREAM INPUT (K MAP<INT, DOUBLE> KEY, foo INT) WITH (kafka_topic='input_topic', format='DELIMITED');
--@test: delimited - MAP - C*AS - key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Map keys, including types that contain maps, are not supported as they may lead to unexpected behavior due to inconsistent serialization. Key column name: `KSQL_COL_0`. Column type: MAP<STRING, STRING>. See https://github.com/confluentinc/ksql/issues/6621 for more.
CREATE STREAM INPUT (k STRING KEY, v STRING) WITH (kafka_topic='input_topic', format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT PARTITION BY MAP(k:=v);
--@test: delimited - STRUCT - C* - key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'DELIMITED' format does not support type 'STRUCT'
CREATE STREAM INPUT (K STRUCT<F1 DOUBLE> KEY, foo INT) WITH (kafka_topic='input_topic', format='DELIMITED');
--@test: delimited - STRUCT - C*AS - key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'DELIMITED' format does not support type 'STRUCT'
CREATE STREAM INPUT (v DOUBLE) WITH (kafka_topic='input_topic', format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT PARTITION BY STRUCT(k := v);
--@test: delimited - deserialize anonymous primitive by default - value - DELIMITED
CREATE STREAM INPUT (K STRING KEY, foo INT) WITH (kafka_topic='input_topic', value_format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (FOO) VALUES (10);
INSERT INTO `INPUT` (K) VALUES ('hi');
ASSERT VALUES `OUTPUT` (FOO) VALUES (10);
ASSERT VALUES `OUTPUT` (K) VALUES ('hi');

--@test: delimited - deserialize DELIMITED with WRAP_SINGLE_VALUE true
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Format 'DELIMITED' does not support 'WRAP_SINGLE_VALUE' set to 'true'.
CREATE STREAM INPUT (K STRING KEY, foo INT) WITH (WRAP_SINGLE_VALUE=true, kafka_topic='input_topic', value_format='DELIMITED');
--@test: delimited - deserialize DELIMITED with WRAP_SINGLE_VALUE false
CREATE STREAM INPUT (K STRING KEY, foo INT) WITH (WRAP_SINGLE_VALUE=false, kafka_topic='input_topic', value_format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, FOO) VALUES ('foo', 1);
ASSERT VALUES `OUTPUT` (K, FOO) VALUES ('foo', 1);

--@test: delimited - select delimited value_format
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE integer) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM S2 as SELECT K, id, name, value FROM test;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, 'zero', 0, 0);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, '100', 100, 0);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100, '100', 500, 0);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100, '100', 100, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, 'zero', 0, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, '100', 100, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100, '100', 500, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100, '100', 100, 0);

--@test: delimited - select delimited value_format into another format - JSON
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE integer) WITH (kafka_topic='test_topic', value_format='DELIMITED', value_delimiter=',');
CREATE STREAM S2 WITH(value_format='JSON') as SELECT K, id, name, value FROM test;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, 'zero', 0, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, 'zero', 0, 0);

--@test: delimited - select delimited value_format into another format - AVRO
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE integer) WITH (kafka_topic='test_topic', value_format='DELIMITED', value_delimiter=',');
CREATE STREAM S2 WITH(value_format='AVRO') as SELECT K, id, name, value FROM test;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, 'zero', 0, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, 'zero', 0, 0);

--@test: delimited - select delimited value_format into another format - PROTOBUF
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE integer) WITH (kafka_topic='test_topic', value_format='DELIMITED', value_delimiter=',');
CREATE STREAM S2 WITH(value_format='PROTOBUF') as SELECT K, id, name, value FROM test;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, 'zero', 0, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, 'zero', 0, 0);

--@test: delimited - select delimited value_format into another format - PROTOBUF_NOSR
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE integer) WITH (kafka_topic='test_topic', value_format='DELIMITED', value_delimiter=',');
CREATE STREAM S2 WITH(value_format='PROTOBUF_NOSR') as SELECT K, id, name, value FROM test;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, 'zero', 0, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, 'zero', 0, 0);

--@test: delimited - validate value_delimiter to be single character
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Failed to prepare statement: Configuration VALUE_DELIMITER is invalid: Invalid delimiter value: '<~>'. Delimiter must be a single character or TAB,SPACE
CREATE STREAM TEST WITH (kafka_topic='test_topic', value_format='DELIMITED', value_delimiter='<~>');
--@test: delimited - validate delimiter is not a space
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Failed to prepare statement: Configuration VALUE_DELIMITER is invalid: Delimiter cannot be empty, if you meant to have a tab or space for delimiter, please use the special values 'TAB' or 'SPACE'
CREATE STREAM TEST WITH (kafka_topic='test_topic', value_format='DELIMITED', value_delimiter=' ');
--@test: delimited - validate delimiter is not a tab character
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Failed to prepare statement: Configuration VALUE_DELIMITER is invalid: Delimiter cannot be empty, if you meant to have a tab or space for delimiter, please use the special values 'TAB' or 'SPACE'
CREATE STREAM TEST WITH (kafka_topic='test_topic', value_format='DELIMITED', value_delimiter='	');
--@test: delimited - select delimited value_format with pipe separated values - should take source delimiter for sink
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE integer) WITH (kafka_topic='test_topic', value_format='DELIMITED', value_delimiter='|');
CREATE STREAM S2 as SELECT K, id, name, value FROM test;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0,'zero',0, 0);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0,'100',100, 0);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100,'100',500, 0);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100,'100',100, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0,'zero',0, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0,'100',100, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100,'100',500, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100,'100',100, 0);

--@test: delimited - select delimited value_format with $ separated values using custom delimiter character
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE integer) WITH (kafka_topic='test_topic', value_format='DELIMITED', value_delimiter='|');
CREATE STREAM S2 WITH(value_delimiter='$') AS SELECT * FROM test;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0,'zero',0,0);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0,'100',100,0);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100,'100',500,0);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100,'100',100,0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0,'zero',0,0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0,'100',100,0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100,'100',500,0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100,'100',100,0);

--@test: delimited - select delimited value_format with SPACE separated values using custom delimiter character
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE integer) WITH (kafka_topic='test_topic', value_format='DELIMITED', value_delimiter='SPACE');
CREATE STREAM S2 as SELECT K, id, name, value FROM test;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, 'zero', 0, 0);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, '100', 100, 0);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100, '100', 500, 0);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100, '100', 100, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, 'zero', 0, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0, '100',100, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100, '100', 500, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100, '100', 100, 0);

--@test: delimited - select delimited value_format with TAB separated values using custom delimiter character
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE integer) WITH (kafka_topic='test_topic', value_format='DELIMITED', value_delimiter='TAB');
CREATE STREAM S2 as SELECT K, id, name, value FROM test;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0,	'zero',	0, 0);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0,	'100',	100, 0);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100,'100',500, 0);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100	,'100',	100, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0	,'zero',	0, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('0', 0	,'100',	100, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100	,'100',	500, 0);
ASSERT VALUES `S2` (K, ID, NAME, VALUE, ROWTIME) VALUES ('100', 100	,'100',	100, 0);

--@test: delimited - ARRAY - C* - value
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'DELIMITED' format does not support type 'ARRAY'
CREATE STREAM INPUT (foo ARRAY<INT>) WITH (kafka_topic='input_topic', value_format='DELIMITED');
--@test: delimited - ARRAY - C*AS - value
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'DELIMITED' format does not support type 'ARRAY'
CREATE STREAM INPUT (v INT) WITH (kafka_topic='input_topic', value_format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT ARRAY[v] FROM INPUT;
--@test: delimited - MAP - C* - value
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'DELIMITED' format does not support type 'MAP'
CREATE STREAM INPUT (foo MAP<INT, DOUBLE>) WITH (kafka_topic='input_topic', value_format='DELIMITED');
--@test: delimited - MAP - C*AS - value
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'DELIMITED' format does not support type 'MAP'
CREATE STREAM INPUT (k INT, v DOUBLE) WITH (kafka_topic='input_topic', value_format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT MAP(k:=v) FROM INPUT;
--@test: delimited - STRUCT - C* - value
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'DELIMITED' format does not support type 'STRUCT'
CREATE STREAM INPUT (foo STRUCT<F1 DOUBLE>) WITH (kafka_topic='input_topic', value_format='DELIMITED');
--@test: delimited - STRUCT - C*AS - value
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'DELIMITED' format does not support type 'STRUCT'
CREATE STREAM INPUT (v DOUBLE) WITH (kafka_topic='input_topic', value_format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT STRUCT(k := v) FROM INPUT;
--@test: delimited - read written decimals
CREATE STREAM INPUT (ID STRING KEY, v DECIMAL(33, 16)) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM INTERMEDIATE AS SELECT * FROM INPUT;
CREATE STREAM OUTPUT AS SELECT * FROM INTERMEDIATE;
INSERT INTO `INPUT` (V) VALUES (12345678987654321.2345678987654321);
INSERT INTO `INPUT` (V) VALUES (.12);
INSERT INTO `INPUT` (V) VALUES (-12345.12);
INSERT INTO `INPUT` (V) VALUES ("-12345.12");
INSERT INTO `INPUT` (V) VALUES (0);
ASSERT VALUES `OUTPUT` (V) VALUES (12345678987654321.2345678987654321);
ASSERT VALUES `OUTPUT` (V) VALUES (0.1200000000000000);
ASSERT VALUES `OUTPUT` (V) VALUES ("-12345.1200000000000000");
ASSERT VALUES `OUTPUT` (V) VALUES ("-12345.1200000000000000");
ASSERT VALUES `OUTPUT` (V) VALUES (0.0000000000000000);

--@test: delimited - keyless
CREATE STREAM INPUT (V INT) WITH (kafka_topic='input_topic', format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (V) VALUES (0);
ASSERT VALUES `OUTPUT` (V) VALUES (0);

