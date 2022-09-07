--@test: between - test BETWEEN with integers
CREATE STREAM TEST (ID STRING KEY, source int) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, source AS THING FROM TEST WHERE source BETWEEN 2 AND 4;
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('0', NULL, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('1', 1, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('2', 2, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('3', 3, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('4', 4, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('5', 5, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('2', 2, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('3', 3, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('4', 4, 0);

--@test: between - test BETWEEN with bigint
CREATE STREAM TEST (ID STRING KEY, source bigint) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, source AS THING FROM TEST WHERE source BETWEEN 2 AND 4;
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('0', NULL, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('1', 1, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('2', 2, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('3', 3, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('4', 4, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('5', 5, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('2', 2, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('3', 3, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('4', 4, 0);

--@test: between - test BETWEEN with floating point
CREATE STREAM TEST (ID STRING KEY, source double) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, source AS THING FROM TEST WHERE source BETWEEN 2 AND 3.9;
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('0', NULL, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('1', 1.9, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('2', 2.0, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('3', 2.1, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('4', 3.9, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('5', 4.0, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('2', 2.0, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('3', 2.1, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('4', 3.9, 0);

--@test: between - test BETWEEN with string values
CREATE STREAM TEST (ID STRING KEY, source string) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, source AS THING FROM TEST WHERE source BETWEEN 'b' AND 'c';
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('0', NULL, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('1', 'a', 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('2', 'b', 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('3', 'ba', 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('4', 'c', 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('5', 'ca', 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('2', 'b', 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('3', 'ba', 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('4', 'c', 0);

--@test: between - test BETWEEN with integers and variable values
CREATE STREAM TEST (ID STRING KEY, source int, min int, max int) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, source AS THING FROM TEST WHERE source BETWEEN min AND max;
INSERT INTO `TEST` (ID, source, min, max, ROWTIME) VALUES ('0', NULL, 0, 2, 0);
INSERT INTO `TEST` (ID, source, min, max, ROWTIME) VALUES ('1', 1, 0, 2, 0);
INSERT INTO `TEST` (ID, source, min, max, ROWTIME) VALUES ('2', 2, NULL, 2, 0);
INSERT INTO `TEST` (ID, source, min, max, ROWTIME) VALUES ('3', 3, 0, NULL, 0);
INSERT INTO `TEST` (ID, source, min, max, ROWTIME) VALUES ('4', 4, NULL, NULL, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('1', 1, 0);

--@test: between - test BETWEEN with integers and expressions
CREATE STREAM TEST (ID STRING KEY, source int, avg int) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, source AS THING FROM TEST WHERE source + 2 BETWEEN avg / 2 AND avg * 2;
INSERT INTO `TEST` (ID, source, avg, ROWTIME) VALUES ('1', NULL, 10, 0);
INSERT INTO `TEST` (ID, source, avg, ROWTIME) VALUES ('2', 1, 10, 0);
INSERT INTO `TEST` (ID, source, avg, ROWTIME) VALUES ('3', 10, 10, 0);
INSERT INTO `TEST` (ID, source, avg, ROWTIME) VALUES ('4', 4, 10, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('3', 10, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('4', 4, 0);

--@test: between - test NOT BETWEEN with integers
CREATE STREAM TEST (ID STRING KEY, source int) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, source AS THING FROM TEST WHERE source NOT BETWEEN 2 AND 4;
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('1', 1, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('2', 2, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('3', 3, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('4', 4, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('5', 5, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('1', 1, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('5', 5, 0);

--@test: between - test BETWEEN with array dereference
CREATE STREAM TEST (ID STRING KEY, source array<int>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, source[2] AS THING FROM TEST WHERE source[2] BETWEEN 2 AND 4;
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('1', ARRAY[10, 1], 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('2', ARRAY[10, 2], 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('3', ARRAY[10, 3], 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('4', ARRAY[10, 4], 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('5', ARRAY[10, 5], 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('2', 2, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('3', 3, 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('4', 4, 0);

--@test: between - test BETWEEN with string values with substring
CREATE STREAM TEST (ID STRING KEY, source string) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, source AS THING FROM TEST WHERE source BETWEEN SUBSTRING('zb',2) AND 'c';
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('0', NULL, 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('1', 'a', 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('2', 'b', 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('3', 'ba', 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('4', 'c', 0);
INSERT INTO `TEST` (ID, source, ROWTIME) VALUES ('5', 'ca', 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('2', 'b', 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('3', 'ba', 0);
ASSERT VALUES `OUTPUT` (ID, THING, ROWTIME) VALUES ('4', 'c', 0);

