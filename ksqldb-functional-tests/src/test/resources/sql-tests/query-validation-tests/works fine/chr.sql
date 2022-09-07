--@test: chr - codepoint from decimal code - AVRO
CREATE STREAM INPUT (id STRING KEY, utfcode INTEGER) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT AS SELECT id, chr(utfcode) AS result FROM INPUT;
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r1', 75);
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r2', 22909);
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r3', 99000);
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r4', -1);
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r5', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', 'K');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r2', '好');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r3', '𘊸');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r4', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', NULL);

--@test: chr - codepoint from decimal code - JSON
CREATE STREAM INPUT (id STRING KEY, utfcode INTEGER) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, chr(utfcode) AS result FROM INPUT;
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r1', 75);
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r2', 22909);
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r3', 99000);
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r4', -1);
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r5', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', 'K');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r2', '好');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r3', '𘊸');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r4', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', NULL);

--@test: chr - codepoint from text code - AVRO
CREATE STREAM INPUT (id STRING KEY, utfcode STRING) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT AS SELECT id, chr(utfcode) AS result FROM INPUT;
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r1', '\u004b');
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r2', '\u597d');
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r3', '\ud820\udeb8');
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r4', '75');
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r5', '004b');
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r6', 'bogus');
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r7', '');
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r8', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', 'K');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r2', '好');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r3', '𘊸');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r4', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r6', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r7', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r8', NULL);

--@test: chr - codepoint from text code - JSON
CREATE STREAM INPUT (id STRING KEY, utfcode STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, chr(utfcode) AS result FROM INPUT;
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r1', '\u004b');
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r2', '\u597d');
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r3', '\ud820\udeb8');
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r4', '75');
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r5', '004b');
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r6', 'bogus');
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r7', '');
INSERT INTO `INPUT` (ID, utfcode) VALUES ('r8', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', 'K');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r2', '好');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r3', '𘊸');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r4', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r6', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r7', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r8', NULL);

--@test: chr - multiple invocations - AVRO
CREATE STREAM INPUT (id STRING KEY, utfcode1 STRING, utfcode2 STRING, utfcode3 INTEGER, utfcode4 INTEGER) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT AS SELECT id, concat(chr(utfcode1), chr(utfcode2), chr(utfcode3), chr(utfcode4)) AS result FROM INPUT;
INSERT INTO `INPUT` (ID, utfcode1, utfcode2, utfcode3, utfcode4) VALUES ('r1', '\u004b', '\u0053', 81, 76);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', 'KSQL');

--@test: chr - multiple invocations - JSON
CREATE STREAM INPUT (id STRING KEY, utfcode1 STRING, utfcode2 STRING, utfcode3 INTEGER, utfcode4 INTEGER) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, concat(chr(utfcode1), chr(utfcode2), chr(utfcode3), chr(utfcode4)) AS result FROM INPUT;
INSERT INTO `INPUT` (ID, utfcode1, utfcode2, utfcode3, utfcode4) VALUES ('r1', '\u004b', '\u0053', 81, 76);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', 'KSQL');

