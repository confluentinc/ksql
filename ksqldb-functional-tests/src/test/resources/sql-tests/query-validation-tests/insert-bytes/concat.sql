--@test: concat - concat_ws - string - JSON
CREATE STREAM INPUT (ID BIGINT KEY, S1 STRING, C1 STRING, C2 STRING, C3 STRING) WITH (kafka_topic='input_topic',value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, CONCAT_WS(S1, C1, C2, C3, NULL, 'literal') AS COMBINED FROM INPUT;
INSERT INTO `INPUT` (ID, S1, C1, C2, C3) VALUES (1, 'SEP', 'foo', 'bar', 'baz');
INSERT INTO `INPUT` (ID, S1, C1, C2, C3) VALUES (2, 'SEP', 'foo', NULL, 'baz');
INSERT INTO `INPUT` (ID, S1, C1, C2, C3) VALUES (3, 'SEP', NULL, NULL, NULL);
INSERT INTO `INPUT` (ID, S1, C1, C2, C3) VALUES (4, NULL, 'foo', 'bar', 'baz');
ASSERT VALUES `OUTPUT` (ID, COMBINED) VALUES (1, 'fooSEPbarSEPbazSEPliteral');
ASSERT VALUES `OUTPUT` (ID, COMBINED) VALUES (2, 'fooSEPbazSEPliteral');
ASSERT VALUES `OUTPUT` (ID, COMBINED) VALUES (3, 'literal');
ASSERT VALUES `OUTPUT` (ID, COMBINED) VALUES (4, NULL);

--@test: concat - concat_ws - bytes - JSON
CREATE STREAM INPUT (ID BIGINT KEY, S1 BYTES, C1 BYTES, C2 BYTES, C3 BYTES) WITH (kafka_topic='input_topic',value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, CONCAT_WS(S1, C1, C2, NULL, C3) AS COMBINED FROM INPUT;
INSERT INTO `INPUT` (ID, S1, C1, C2, C3) VALUES (1, 'YQ==', 'Yg==', 'eWVz', 'bm8=');
INSERT INTO `INPUT` (ID, S1, C1, C2, C3) VALUES (2, 'YQ==', 'Yg==', '', 'bm8=');
INSERT INTO `INPUT` (ID, S1, C1, C2, C3) VALUES (3, 'YQ==', NULL, NULL, NULL);
INSERT INTO `INPUT` (ID, S1, C1, C2, C3) VALUES (4, NULL, 'Yg==', 'eWVz', 'bm8=');
ASSERT VALUES `OUTPUT` (ID, COMBINED) VALUES (1, 'YmF5ZXNhbm8=');
ASSERT VALUES `OUTPUT` (ID, COMBINED) VALUES (2, 'YmFhbm8=');
ASSERT VALUES `OUTPUT` (ID, COMBINED) VALUES (3, '');
ASSERT VALUES `OUTPUT` (ID, COMBINED) VALUES (4, NULL);

--@test: concat - concat - bytes
CREATE STREAM TEST (ID STRING KEY, C1 BYTES, C2 BYTES) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, CONCAT(C1, NULL, C2) AS THING FROM TEST;
INSERT INTO `TEST` (C1, C2) VALUES ('eWVz', 'bm8=');
INSERT INTO `TEST` (C1, C2) VALUES ('', 'bm8=');
ASSERT VALUES `OUTPUT` (THING) VALUES ('eWVzbm8=');
ASSERT VALUES `OUTPUT` (THING) VALUES ('bm8=');

--@test: concat - concat fields using CONCAT
CREATE STREAM TEST (ID STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, CONCAT('prefix-', source, '-postfix') AS THING FROM TEST;
INSERT INTO `TEST` (source) VALUES ('s1');
INSERT INTO `TEST` (source) VALUES ('s2');
ASSERT VALUES `OUTPUT` (THING) VALUES ('prefix-s1-postfix');
ASSERT VALUES `OUTPUT` (THING) VALUES ('prefix-s2-postfix');

--@test: concat - concat fields using '+' operator
CREATE STREAM TEST (ID STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, 'prefix-' + source + '-postfix' AS THING FROM TEST;
INSERT INTO `TEST` (source) VALUES ('s1');
INSERT INTO `TEST` (source) VALUES ('s2');
ASSERT VALUES `OUTPUT` (THING) VALUES ('prefix-s1-postfix');
ASSERT VALUES `OUTPUT` (THING) VALUES ('prefix-s2-postfix');

--@test: concat - should handle characters the must be escaped in java
CREATE STREAM INPUT (ID STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, CONCAT('"', CONCAT(source, '\')) AS THING FROM INPUT;
INSERT INTO `INPUT` (source) VALUES ('foo');
INSERT INTO `INPUT` (source) VALUES ('\foo"');
ASSERT VALUES `OUTPUT` (THING) VALUES ('"foo\');
ASSERT VALUES `OUTPUT` (THING) VALUES ('"\foo"\');

--@test: concat - should handle characters the must be escaped in sql
CREATE STREAM INPUT (ID STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, CONCAT('''', CONCAT(source, '''')) AS THING FROM INPUT;
INSERT INTO `INPUT` (source) VALUES ('foo');
ASSERT VALUES `OUTPUT` (THING) VALUES ('''foo''');

