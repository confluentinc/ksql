--@test: table - update-delete
CREATE TABLE TEST (K STRING PRIMARY KEY, ID bigint, NAME varchar, VALUE int) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE T1 as SELECT K, NAME, VALUE FROM test;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('1', 1, 'one', 100, 10);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('1', 2, 'two', 200, 20);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('1', 3, 'three', 300, 30);
ASSERT VALUES `T1` (K, NAME, VALUE) VALUES ('1', 'one', 100);
ASSERT VALUES `T1` (K, NAME, VALUE) VALUES ('1', 'two', 200);
ASSERT VALUES `T1` (K, NAME, VALUE) VALUES ('1', 'three', 300);

--@test: table - should not reuse source topic for change log by default
CREATE TABLE TEST (K STRING PRIMARY KEY, ID bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE T1 as SELECT * FROM test;
INSERT INTO `TEST` (K, ID) VALUES ('1', 2);
ASSERT VALUES `T1` (K, ID) VALUES ('1', 2);

--@test: table - should NOT reuse source topic for change log if topology optimizations are off
SET 'ksql.streams.topology.optimization' = 'none';CREATE TABLE TEST (K STRING PRIMARY KEY, ID bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE T1 as SELECT * FROM test;
INSERT INTO `TEST` (K, ID) VALUES ('1', 2);
ASSERT VALUES `T1` (K, ID) VALUES ('1', 2);

--@test: table - should forward nulls in changelog when table not materialized
CREATE TABLE INPUT (K STRING PRIMARY KEY, ID bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT as SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, ID) VALUES ('1', 1);
INSERT INTO `INPUT` (K) VALUES ('1');
INSERT INTO `INPUT` (K, ID) VALUES ('1', 2);
ASSERT VALUES `OUTPUT` (K, ID) VALUES ('1', 1);
ASSERT VALUES `OUTPUT` (K) VALUES ('1');
ASSERT VALUES `OUTPUT` (K, ID) VALUES ('1', 2);

--@test: table - should not blow up on null key
CREATE TABLE INPUT (K STRING PRIMARY KEY, ID bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT as SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, ID) VALUES ('1', 1);
INSERT INTO `INPUT` (K, ID) VALUES (NULL, 2);
INSERT INTO `INPUT` (K, ID) VALUES ('1', 3);
ASSERT VALUES `OUTPUT` (K, ID) VALUES ('1', 1);
ASSERT VALUES `OUTPUT` (K, ID) VALUES ('1', 3);

