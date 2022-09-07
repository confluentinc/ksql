--@test: none - as key format of keyless stream
CREATE STREAM INPUT (foo INT) WITH (kafka_topic='input_topic', key_format='NONE', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (foo) VALUES (10);
INSERT INTO `INPUT` (foo) VALUES (11);
INSERT INTO `INPUT` (foo) VALUES (NULL);
ASSERT VALUES `OUTPUT` (FOO) VALUES (10);
ASSERT VALUES `OUTPUT` (FOO) VALUES (11);
ASSERT VALUES `OUTPUT` (FOO) VALUES (NULL);

--@test: none - as key format of keyed stream
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'NONE' format can only be used when no columns are defined. Got: [`K` STRING KEY]
CREATE STREAM INPUT (K STRING KEY, foo INT) WITH (kafka_topic='input_topic', key_format='NONE', value_format='JSON');
--@test: none - as key format of table
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'NONE' format can only be used when no columns are defined. Got: [`K` STRING KEY]
CREATE TABLE INPUT (K STRING PRIMARY KEY, foo INT) WITH (kafka_topic='input_topic', key_format='NONE', value_format='JSON');
--@test: none - as value format with value columns
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'NONE' format can only be used when no columns are defined. Got: [`FOO` INTEGER]
CREATE STREAM INPUT (foo INT) WITH (kafka_topic='input_topic', value_format='NONE');
--@test: none - in CSAS from keyless stream
CREATE STREAM INPUT (foo INT) WITH (kafka_topic='input_topic', key_format='KAFKA', value_format='JSON');
CREATE STREAM OUTPUT WITH(key_format='NONE') AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (FOO) VALUES (10);
INSERT INTO `INPUT` (foo) VALUES (NULL);
ASSERT VALUES `OUTPUT` (FOO) VALUES (10);
ASSERT VALUES `OUTPUT` (FOO) VALUES (NULL);

--@test: none - in CSAS partitioning by null
CREATE STREAM INPUT (ID INT KEY, foo INT) WITH (kafka_topic='input_topic', key_format='KAFKA', value_format='JSON');
CREATE STREAM OUTPUT WITH(key_format='NONE') AS SELECT * FROM INPUT PARTITION BY null;
INSERT INTO `INPUT` (ID, FOO) VALUES (9, 10);
INSERT INTO `INPUT` (ID) VALUES (22);
ASSERT VALUES `OUTPUT` (FOO, ID) VALUES (10, 9);
ASSERT VALUES `OUTPUT` (Id, FOO) VALUES (22, NULL);

--@test: none - explicitly set in CSAS with key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'NONE' format can only be used when no columns are defined. Got: [`BAR` INTEGER KEY]
CREATE STREAM INPUT (foo INT, bar INT) WITH (kafka_topic='input_topic', key_format='KAFKA', value_format='JSON');
CREATE STREAM OUTPUT WITH (key_format='NONE') AS SELECT * FROM INPUT PARTITION BY BAR;
--@test: none - explicitly set in CTAS
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The 'NONE' format can only be used when no columns are defined. Got: [`K` INTEGER KEY]
CREATE STREAM INPUT (foo INT, bar INT) WITH (kafka_topic='input_topic', key_format='KAFKA', value_format='JSON');
CREATE TABLE OUTPUT WITH (key_format='NONE') AS SELECT BAR AS K, COUNT() FROM INPUT GROUP BY BAR;
--@test: none - inherited in CSAS - PARTITION BY
SET 'ksql.persistence.default.format.key' = 'JSON';
CREATE STREAM INPUT (foo INT, bar INT) WITH (kafka_topic='input_topic', key_format='NONE', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT PARTITION BY BAR;
INSERT INTO `INPUT` (foo) VALUES (10);
INSERT INTO `INPUT` (foo) VALUES (11);
INSERT INTO `INPUT` (bar) VALUES (3);
ASSERT VALUES `OUTPUT` (FOO) VALUES (10);
ASSERT VALUES `OUTPUT` (FOO) VALUES (11);
ASSERT VALUES `OUTPUT` (bar) VALUES (3);

--@test: none - inherited in CSAS - JOIN with repartition
CREATE TABLE T (ID INT PRIMARY KEY, VAL STRING) WITH (kafka_topic='t', key_format='KAFKA', value_format='JSON');
CREATE STREAM S (ID INT) WITH (kafka_topic='s', key_format='NONE', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT S.ID, VAL FROM S JOIN T ON S.ID = T.ID;
INSERT INTO `T` (ID, VAL) VALUES (10, 'hello');
INSERT INTO `S` (id) VALUES (10);
ASSERT VALUES `OUTPUT` (S_ID, VAL) VALUES (10, 'hello');

--@test: none - inherited in CTAS - GROUP BY
SET 'ksql.persistence.default.format.key' = 'JSON';CREATE STREAM INPUT (foo INT) WITH (kafka_topic='input_topic', key_format='NONE', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT FOO, COUNT() AS COUNT FROM INPUT GROUP BY FOO;
INSERT INTO `INPUT` (foo) VALUES (22);
INSERT INTO `INPUT` (foo) VALUES (null);
ASSERT VALUES `OUTPUT` (FOO, COUNT) VALUES (22, 1);

