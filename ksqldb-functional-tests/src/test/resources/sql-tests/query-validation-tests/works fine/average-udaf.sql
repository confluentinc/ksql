--@test: average-udaf - average int
CREATE STREAM INPUT (ID STRING KEY, VALUE integer) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT ID, avg(value) AS avg FROM INPUT group by ID;
INSERT INTO `INPUT` (ID, value) VALUES ('alice', 1);
INSERT INTO `INPUT` (ID, value) VALUES ('bob', 2);
INSERT INTO `INPUT` (ID, value) VALUES ('alice', 2);
INSERT INTO `INPUT` (ID, value) VALUES ('alice', 2);
INSERT INTO `INPUT` (ID, value) VALUES ('bob', 2);
INSERT INTO `INPUT` (ID, value) VALUES ('bob', 0);
INSERT INTO `INPUT` (ID, value) VALUES ('alice', NULL);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('alice', 1.0);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('bob', 2.0);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('alice', 1.5);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('alice', 1.6666666666666667);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('bob', 2.0);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('bob', 1.3333333333333333);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('alice', 1.6666666666666667);

--@test: average-udaf - average long
CREATE STREAM INPUT (ID STRING KEY, VALUE bigint) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT ID, avg(value) AS avg FROM INPUT group by ID;
INSERT INTO `INPUT` (ID, value) VALUES ('alice', -1);
INSERT INTO `INPUT` (ID, value) VALUES ('bob', 2);
INSERT INTO `INPUT` (ID, value) VALUES ('alice', 9223372036854775807);
INSERT INTO `INPUT` (ID, value) VALUES ('alice', 1);
INSERT INTO `INPUT` (ID, value) VALUES ('bob', -2);
INSERT INTO `INPUT` (ID, value) VALUES ('bob', 0);
INSERT INTO `INPUT` (ID, value) VALUES ('alice', NULL);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('alice', -1.0);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('bob', 2.0);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('alice', 4611686018427387900);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('alice', 3074457345618258400);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('bob', 0.0);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('bob', 0.0);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('alice', 3074457345618258400);

--@test: average-udaf - average double
CREATE STREAM INPUT (ID STRING KEY, VALUE double) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT ID, avg(value) AS avg FROM INPUT group by ID;
INSERT INTO `INPUT` (ID, value) VALUES ('alice', -1.8);
INSERT INTO `INPUT` (ID, value) VALUES ('bob', 2.3);
INSERT INTO `INPUT` (ID, value) VALUES ('alice', 9223372036854.775807);
INSERT INTO `INPUT` (ID, value) VALUES ('alice', 100.2);
INSERT INTO `INPUT` (ID, value) VALUES ('bob', -200000.6);
INSERT INTO `INPUT` (ID, value) VALUES ('bob', 0.0);
INSERT INTO `INPUT` (ID, value) VALUES ('alice', NULL);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('alice', -1.8);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('bob', 2.3);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('alice', 4611686018426.487);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('alice', 3074457345651.058);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('bob', -99999.15000000001);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('bob', -66666.1);
ASSERT VALUES `OUTPUT` (ID, AVG) VALUES ('alice', 3074457345651.058);

--@test: average-udaf - average - DELIMITED
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: One of the functions used in the statement has an intermediate type that the value format can not handle. Please remove the function or change the format.
CREATE STREAM INPUT (ID STRING KEY, VALUE integer) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT ID, avg(value) AS avg FROM INPUT group by ID;
--@test: average-udaf - average udaf with table
CREATE TABLE INPUT (ID STRING PRIMARY KEY, K STRING, VALUE integer) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT K, avg(value) AS avg FROM INPUT group by K;
INSERT INTO `INPUT` (ID, K, value) VALUES ('alice', 'a', 1);
INSERT INTO `INPUT` (ID, K, value) VALUES ('bob', 'a', 2);
INSERT INTO `INPUT` (ID, K, value) VALUES ('alice', 'a', NULL);
INSERT INTO `INPUT` (ID, K, value) VALUES ('alice', 'a', 4);
ASSERT VALUES `OUTPUT` (K, AVG) VALUES ('a', 1.0);
ASSERT VALUES `OUTPUT` (K, AVG) VALUES ('a', 1.5);
ASSERT VALUES `OUTPUT` (K, AVG) VALUES ('a', 2.0);
ASSERT VALUES `OUTPUT` (K, AVG) VALUES ('a', 2.0);
ASSERT VALUES `OUTPUT` (K, AVG) VALUES ('a', 2.0);
ASSERT VALUES `OUTPUT` (K, AVG) VALUES ('a', 3.0);

