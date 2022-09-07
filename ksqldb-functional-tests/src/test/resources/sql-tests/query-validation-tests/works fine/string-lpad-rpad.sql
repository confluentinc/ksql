--@test: string-lpad-rpad - LPad with all args from record - JSON
CREATE STREAM INPUT (id STRING KEY, subject STRING, len INT, padding STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, lpad(subject, len, padding) as result FROM INPUT;
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r1', 'foo', 7, 'Bar');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r2', 'foo', 5, 'Bar');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r3', 'foo', 2, 'Bar');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r4', 'foo', -1, 'Bar');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r5', 'foo', 5, NULL);
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r6', 'foo', NULL, 'Bar');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r7', NULL, -5, 'Bar');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r8', 'foo', 7, '');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r9', '', 7, 'Bar');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', 'BarBfoo');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r2', 'Bafoo');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r3', 'fo');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r4', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r6', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r7', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r8', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r9', 'BarBarB');

--@test: string-lpad-rpad - LPad with literal args - JSON
CREATE STREAM INPUT (id STRING KEY) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, lpad('foo', 7, 'Bar') as result FROM INPUT;
INSERT INTO `INPUT` (ID) VALUES ('r1');
INSERT INTO `INPUT` (ID) VALUES ('r2');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', 'BarBfoo');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r2', 'BarBfoo');

--@test: string-lpad-rpad - RPad with all args from record - JSON
CREATE STREAM INPUT (id STRING KEY, subject STRING, len INT, padding STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, rpad(subject, len, padding) as result FROM INPUT;
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r1', 'foo', 7, 'Bar');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r2', 'foo', 5, 'Bar');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r3', 'foo', 2, 'Bar');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r4', 'foo', -1, 'Bar');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r5', 'foo', 5, NULL);
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r6', 'foo', NULL, 'Bar');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r7', NULL, -5, 'Bar');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r8', 'foo', 7, '');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r9', '', 7, 'Bar');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', 'fooBarB');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r2', 'fooBa');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r3', 'fo');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r4', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r6', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r7', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r8', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r9', 'BarBarB');

--@test: string-lpad-rpad - RPad with literal args - JSON
CREATE STREAM INPUT (id STRING KEY) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, rpad('foo', 7, 'Bar') as result FROM INPUT;
INSERT INTO `INPUT` (ID) VALUES ('r1');
INSERT INTO `INPUT` (ID) VALUES ('r2');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', 'fooBarB');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r2', 'fooBarB');

