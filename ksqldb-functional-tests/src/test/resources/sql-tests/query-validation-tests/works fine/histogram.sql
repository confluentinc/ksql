--@test: histogram - histogram string - AVRO
CREATE STREAM TEST (ID BIGINT KEY, VALUE varchar) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE S2 as SELECT ID, histogram(value) as counts FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'foo');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'bar');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'foo');
ASSERT VALUES `S2` (ID, COUNTS) VALUES (0, MAP('foo':=1));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (100, MAP('baz':=1));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (0, MAP('foo':=1, 'bar':=1));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (100, MAP('baz':=2));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (100, MAP('baz':=2, 'foo':=1));

--@test: histogram - histogram string - JSON
CREATE STREAM TEST (ID BIGINT KEY, VALUE varchar) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, histogram(value) as counts FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'foo');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'bar');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'foo');
ASSERT VALUES `S2` (ID, COUNTS) VALUES (0, MAP('foo':=1));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (100, MAP('baz':=1));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (0, MAP('foo':=1, 'bar':=1));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (100, MAP('baz':=2));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (100, MAP('baz':=2, 'foo':=1));

--@test: histogram - histogram string - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, VALUE varchar) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE TABLE S2 as SELECT ID, histogram(value) as counts FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'foo');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'bar');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'foo');
ASSERT VALUES `S2` (ID, COUNTS) VALUES (0, MAP('foo':=1));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (100, MAP('baz':=1));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (0, MAP('foo':=1, 'bar':=1));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (100, MAP('baz':=2));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (100, MAP('baz':=2, 'foo':=1));

--@test: histogram - histogram string - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, VALUE varchar) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE TABLE S2 as SELECT ID, histogram(value) as counts FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'foo');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'bar');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'foo');
ASSERT VALUES `S2` (ID, COUNTS) VALUES (0, MAP('foo':=1));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (100, MAP('baz':=1));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (0, MAP('foo':=1, 'bar':=1));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (100, MAP('baz':=2));
ASSERT VALUES `S2` (ID, COUNTS) VALUES (100, MAP('baz':=2, 'foo':=1));

--@test: histogram - histogram on a table - AVRO
CREATE TABLE TEST (K STRING PRIMARY KEY, ID bigint, NAME varchar, REGION string) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE COUNT_BY_REGION AS SELECT region, histogram(name) AS COUNTS FROM TEST GROUP BY region;
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('0', 0, 'alice', 'east');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('1', 1, 'bob', 'east');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('2', 2, 'carol', 'west');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('3', 3, 'dave', 'west');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('1', 1, 'bob', 'west');
INSERT INTO `TEST` (K) VALUES ('1');
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('east', MAP('alice':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('east', MAP('alice':=1, 'bob':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1, 'dave':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('east', MAP('alice':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1, 'dave':=1, 'bob':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1, 'dave':=1));

--@test: histogram - histogram on a table - JSON
CREATE TABLE TEST (K STRING PRIMARY KEY, ID bigint, NAME varchar, REGION string) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE COUNT_BY_REGION AS SELECT region, histogram(name) AS COUNTS FROM TEST GROUP BY region;
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('0', 0, 'alice', 'east');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('1', 1, 'bob', 'east');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('2', 2, 'carol', 'west');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('3', 3, 'dave', 'west');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('1', 1, 'bob', 'west');
INSERT INTO `TEST` (K) VALUES ('1');
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('east', MAP('alice':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('east', MAP('alice':=1, 'bob':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1, 'dave':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('east', MAP('alice':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1, 'dave':=1, 'bob':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1, 'dave':=1));

--@test: histogram - histogram on a table - PROTOBUF
CREATE TABLE TEST (K STRING PRIMARY KEY, ID bigint, NAME varchar, REGION string) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE TABLE COUNT_BY_REGION AS SELECT region, histogram(name) AS COUNTS FROM TEST GROUP BY region;
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('0', 0, 'alice', 'east');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('1', 1, 'bob', 'east');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('2', 2, 'carol', 'west');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('3', 3, 'dave', 'west');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('1', 1, 'bob', 'west');
INSERT INTO `TEST` (K) VALUES ('1');
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('east', MAP('alice':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('east', MAP('alice':=1, 'bob':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1, 'dave':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('east', MAP('alice':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1, 'dave':=1, 'bob':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1, 'dave':=1));

--@test: histogram - histogram on a table - PROTOBUF_NOSR
CREATE TABLE TEST (K STRING PRIMARY KEY, ID bigint, NAME varchar, REGION string) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE TABLE COUNT_BY_REGION AS SELECT region, histogram(name) AS COUNTS FROM TEST GROUP BY region;
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('0', 0, 'alice', 'east');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('1', 1, 'bob', 'east');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('2', 2, 'carol', 'west');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('3', 3, 'dave', 'west');
INSERT INTO `TEST` (K, ID, NAME, REGION) VALUES ('1', 1, 'bob', 'west');
INSERT INTO `TEST` (K) VALUES ('1');
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('east', MAP('alice':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('east', MAP('alice':=1, 'bob':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1, 'dave':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('east', MAP('alice':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1, 'dave':=1, 'bob':=1));
ASSERT VALUES `COUNT_BY_REGION` (REGION, COUNTS) VALUES ('west', MAP('carol':=1, 'dave':=1));

