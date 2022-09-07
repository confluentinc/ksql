--@test: correlation-udaf - correlation double
CREATE STREAM INPUT (ID STRING KEY, X double, Y double) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT ID, correlation(X, Y) AS correlation FROM INPUT group by ID;
INSERT INTO `INPUT` (ID, x, y) VALUES ('alice', 1.5, 8.0);
INSERT INTO `INPUT` (ID, x, y) VALUES ('bob', -3.5, -2.0);
INSERT INTO `INPUT` (ID, x, y) VALUES ('alice', -1.0, -2.0);
INSERT INTO `INPUT` (ID, x, y) VALUES ('alice', -4.5, -1.0);
INSERT INTO `INPUT` (ID, x, y) VALUES ('bob', 13.1, NULL);
INSERT INTO `INPUT` (ID, x, y) VALUES ('bob', 3.0, 11.0);
INSERT INTO `INPUT` (ID, x, y) VALUES ('alice', NULL, -6.0);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('alice', NaN);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('bob', NaN);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('alice', 1.0);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('alice', 0.7580764521133173);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('bob', NaN);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('bob', 1.0);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('alice', 0.7580764521133173);

--@test: correlation-udaf - correlation int
CREATE STREAM INPUT (ID STRING KEY, X integer, Y integer) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT ID, correlation(X, Y) AS correlation FROM INPUT group by ID;
INSERT INTO `INPUT` (ID, x, y) VALUES ('alice', 1, 8);
INSERT INTO `INPUT` (ID, x, y) VALUES ('bob', -3, -2);
INSERT INTO `INPUT` (ID, x, y) VALUES ('alice', -1, -2);
INSERT INTO `INPUT` (ID, x, y) VALUES ('alice', -4, -1);
INSERT INTO `INPUT` (ID, x, y) VALUES ('bob', 13, NULL);
INSERT INTO `INPUT` (ID, x, y) VALUES ('bob', 3, 11);
INSERT INTO `INPUT` (ID, x, y) VALUES ('alice', NULL, -6);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('alice', NaN);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('bob', NaN);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('alice', 1.0);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('alice', 0.7455284088780169);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('bob', NaN);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('bob', 1.0);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('alice', 0.7455284088780169);

--@test: correlation-udaf - correlation long
CREATE STREAM INPUT (ID STRING KEY, X bigint, Y bigint) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT ID, correlation(X, Y) AS correlation FROM INPUT group by ID;
INSERT INTO `INPUT` (ID, x, y) VALUES ('alice', 1, 8);
INSERT INTO `INPUT` (ID, x, y) VALUES ('bob', -3, -2);
INSERT INTO `INPUT` (ID, x, y) VALUES ('alice', -1, -2);
INSERT INTO `INPUT` (ID, x, y) VALUES ('alice', -4, -1);
INSERT INTO `INPUT` (ID, x, y) VALUES ('bob', 13, NULL);
INSERT INTO `INPUT` (ID, x, y) VALUES ('bob', 3, 11);
INSERT INTO `INPUT` (ID, x, y) VALUES ('alice', NULL, -6);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('alice', NaN);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('bob', NaN);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('alice', 1.0);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('alice', 0.7455284088780169);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('bob', NaN);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('bob', 1.0);
ASSERT VALUES `OUTPUT` (ID, CORRELATION) VALUES ('alice', 0.7455284088780169);

--@test: correlation-udaf - correlation - DELIMITED
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: One of the functions used in the statement has an intermediate type that the value format can not handle. Please remove the function or change the format.
CREATE STREAM INPUT (ID STRING KEY, X double, Y double) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE OUTPUT AS SELECT ID, correlation(X, Y) AS correlation FROM INPUT group by ID;

--@test: correlation-udaf - correlation udaf with table
CREATE TABLE INPUT (ID STRING PRIMARY KEY, K STRING, X DOUBLE, Y DOUBLE) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT K, correlation(X, Y) AS correlation FROM INPUT group by K;
INSERT INTO `INPUT` (ID, K, x, y) VALUES ('alice', 'a', 1.5, 8.0);
INSERT INTO `INPUT` (ID, K, x, y) VALUES ('bob', 'a', -3.5, -2.0);
INSERT INTO `INPUT` (ID, K, x, y) VALUES ('alice', 'a', -1.0, -2.0);
INSERT INTO `INPUT` (ID, K, x, y) VALUES ('charlie', 'a', -4.5, -1.0);
ASSERT VALUES `OUTPUT` (K, CORRELATION) VALUES ('a', NaN);
ASSERT VALUES `OUTPUT` (K, CORRELATION) VALUES ('a', 1.0);
ASSERT VALUES `OUTPUT` (K, CORRELATION) VALUES ('a', NaN);
ASSERT VALUES `OUTPUT` (K, CORRELATION) VALUES ('a', NaN);
ASSERT VALUES `OUTPUT` (K, CORRELATION) VALUES ('a', -0.7205766921228921);

