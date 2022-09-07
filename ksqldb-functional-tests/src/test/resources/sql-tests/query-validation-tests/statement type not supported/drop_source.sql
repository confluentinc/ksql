--@test: drop_source - drop an existing stream should succeed
CREATE STREAM input2 (K STRING KEY, data VARCHAR) WITH (kafka_topic='input', value_format='DELIMITED');
DROP STREAM input2;
CREATE STREAM input (K STRING KEY, data VARCHAR) WITH (kafka_topic='input', value_format='DELIMITED');
CREATE STREAM output AS SELECT * FROM input;
INSERT INTO `INPUT` (K, DATA) VALUES ('k1', 'v1');
ASSERT VALUES `OUTPUT` (K, DATA) VALUES ('k1', 'v1');

--@test: drop_source - drop a non-existing stream should fail
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Stream TEST does not exist
DROP STREAM test;
--@test: drop_source - drop if exists a non-existing stream should succeed
DROP STREAM IF EXISTS test;
CREATE STREAM input (K STRING KEY, data VARCHAR) WITH (kafka_topic='input', value_format='DELIMITED');
CREATE STREAM output AS SELECT * FROM input;
INSERT INTO `INPUT` (K, DATA) VALUES ('k1', 'v1');
ASSERT VALUES `OUTPUT` (K, DATA) VALUES ('k1', 'v1');

--@test: drop_source - drop if exists an existing stream should succeed
CREATE STREAM input2 (K STRING KEY, data VARCHAR) WITH (kafka_topic='input', value_format='DELIMITED');
DROP STREAM IF EXISTS input2;
CREATE STREAM input (K STRING KEY, data VARCHAR) WITH (kafka_topic='input', value_format='DELIMITED');
CREATE STREAM output AS SELECT * FROM input;
INSERT INTO `INPUT` (K, DATA) VALUES ('k1', 'v1');
ASSERT VALUES `OUTPUT` (K, DATA) VALUES ('k1', 'v1');

