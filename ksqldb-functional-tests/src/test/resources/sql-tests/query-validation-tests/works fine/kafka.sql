--@test: kafka - Wrapped single values
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Format 'KAFKA' does not support 'WRAP_SINGLE_VALUE' set to 'true'.
CREATE STREAM INPUT (K STRING KEY, foo INT) WITH (WRAP_SINGLE_VALUE=true, kafka_topic='input', value_format='KAFKA');
--@test: kafka - Unwrapped single values
CREATE STREAM INPUT (K STRING KEY, foo INT) WITH (WRAP_SINGLE_VALUE=false, kafka_topic='input', value_format='KAFKA');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, FOO) VALUES ('foo', 1);
ASSERT VALUES `OUTPUT` (K, FOO) VALUES ('foo', 1);

--@test: kafka - Default single value wrapping
CREATE STREAM INPUT (K STRING KEY, foo INT) WITH (kafka_topic='input', value_format='KAFKA');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, FOO) VALUES ('foo', 1);
ASSERT VALUES `OUTPUT` (K, FOO) VALUES ('foo', 1);

--@test: kafka - STRING
CREATE STREAM INPUT (K STRING KEY, foo STRING) WITH (kafka_topic='input', value_format='KAFKA');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, FOO) VALUES ('foo', 'bar');
ASSERT VALUES `OUTPUT` (K, FOO) VALUES ('foo', 'bar');

--@test: kafka - INT
CREATE STREAM INPUT (K INT KEY, foo INT) WITH (kafka_topic='input', value_format='KAFKA');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, FOO) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (K, FOO) VALUES (1, 2);

--@test: kafka - BIGINT
CREATE STREAM INPUT (K BIGINT KEY, foo BIGINT) WITH (kafka_topic='input', value_format='KAFKA');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, FOO) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (K, FOO) VALUES (1, 2);

--@test: kafka - DOUBLE
CREATE STREAM INPUT (K DOUBLE KEY, foo DOUBLE) WITH (kafka_topic='input', value_format='KAFKA');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, FOO) VALUES (1.1, 2.2);
ASSERT VALUES `OUTPUT` (K, FOO) VALUES (1.1, 2.2);

--@test: kafka - ARRAY - C* - value
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'KAFKA' format does not support type 'ARRAY'
CREATE STREAM INPUT (foo ARRAY<INT>) WITH (kafka_topic='input_topic', value_format='KAFKA');
--@test: kafka - ARRAY - C*AS - value
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'KAFKA' format does not support type 'ARRAY'
CREATE STREAM INPUT (v INT) WITH (kafka_topic='input_topic', value_format='KAFKA');
CREATE STREAM OUTPUT AS SELECT ARRAY[v] FROM INPUT;
--@test: kafka - MAP - C* - value
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'KAFKA' format does not support type 'MAP'
CREATE STREAM INPUT (foo MAP<INT, DOUBLE>) WITH (kafka_topic='input_topic', value_format='KAFKA');
--@test: kafka - MAP - C*AS - value
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'KAFKA' format does not support type 'MAP'
CREATE STREAM INPUT (k INT, v DOUBLE) WITH (kafka_topic='input_topic', value_format='JSON');
CREATE STREAM OUTPUT WITH (value_format='KAFKA') AS SELECT MAP(k:=v) FROM INPUT;
--@test: kafka - STRUCT - C* - value
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'KAFKA' format does not support type 'STRUCT'
CREATE STREAM INPUT (foo STRUCT<F1 DOUBLE>) WITH (kafka_topic='input_topic', value_format='KAFKA');
--@test: kafka - STRUCT - C*AS - value
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'KAFKA' format does not support type 'STRUCT'
CREATE STREAM INPUT (v DOUBLE) WITH (kafka_topic='input_topic', value_format='KAFKA');
CREATE STREAM OUTPUT AS SELECT STRUCT(k := v) FROM INPUT;
--@test: kafka - ARRAY - C* - key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'KAFKA' format does not support type 'ARRAY'
CREATE STREAM INPUT (K ARRAY<INT> KEY, foo INT) WITH (kafka_topic='input_topic', format='KAFKA');
--@test: kafka - ARRAY - C*AS - key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'KAFKA' format does not support type 'ARRAY'
CREATE STREAM INPUT (v INT) WITH (kafka_topic='input_topic', format='KAFKA');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT PARTITION BY ARRAY[v];
--@test: kafka - MAP - C* - key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Map keys, including types that contain maps, are not supported as they may lead to unexpected behavior due to inconsistent serialization. Key column name: `K`. Column type: MAP<INTEGER, DOUBLE>. See https://github.com/confluentinc/ksql/issues/6621 for more.
CREATE STREAM INPUT (K MAP<INT, DOUBLE> KEY, foo INT) WITH (kafka_topic='input_topic', format='KAFKA');
--@test: kafka - MAP - C*AS - key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Map keys, including types that contain maps, are not supported as they may lead to unexpected behavior due to inconsistent serialization. Key column name: `KSQL_COL_0`. Column type: MAP<STRING, STRING>. See https://github.com/confluentinc/ksql/issues/6621 for more.
CREATE STREAM INPUT (k STRING KEY, v STRING) WITH (kafka_topic='input_topic', format='KAFKA');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT PARTITION BY MAP(k:=v);
--@test: kafka - STRUCT - C* - key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'KAFKA' format does not support type 'STRUCT'
CREATE STREAM INPUT (K STRUCT<F1 DOUBLE> KEY, foo INT) WITH (kafka_topic='input_topic', format='KAFKA');
--@test: kafka - STRUCT - C*AS - key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'KAFKA' format does not support type 'STRUCT'
CREATE STREAM INPUT (v DOUBLE) WITH (kafka_topic='input_topic', format='KAFKA');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT PARTITION BY STRUCT(k := v);
