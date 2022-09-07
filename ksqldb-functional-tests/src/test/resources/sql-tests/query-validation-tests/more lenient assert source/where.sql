--@test: where - on key column
CREATE STREAM INPUT (id int KEY, name STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE id < 10;
INSERT INTO `INPUT` (ID, name) VALUES (8, 'a');
INSERT INTO `INPUT` (ID, name) VALUES (9, 'a');
INSERT INTO `INPUT` (ID, name) VALUES (10, 'b');
INSERT INTO `INPUT` (ID, name) VALUES (11, 'c');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (8, 'a');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (9, 'a');
ASSERT stream OUTPUT (ID INT KEY, NAME STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: where - on value column
CREATE STREAM INPUT (id int KEY, name STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE name not like '%not%';
INSERT INTO `INPUT` (ID, name) VALUES (8, 'this one');
INSERT INTO `INPUT` (ID, name) VALUES (9, 'not this one');
INSERT INTO `INPUT` (ID, name) VALUES (10, 'and this one');
INSERT INTO `INPUT` (ID, name) VALUES (11, 'but not this one');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (8, 'this one');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (10, 'and this one');
ASSERT stream OUTPUT (ID INT KEY, NAME STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: where - on unknown column
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: WHERE column 'BOB' cannot be resolved.
CREATE STREAM INPUT (id int KEY, name STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ABS(BOB) < 10;
