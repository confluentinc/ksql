--@test: bytes-lpad-rpad - LPAD with all args from record - JSON
CREATE STREAM INPUT (id STRING KEY, subject BYTES, len INT, padding BYTES) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, lpad(subject, len, padding) AS result FROM INPUT;
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r1', 'eWVz', 8, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r2', 'eWVz', 5, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r3', 'eWVz', 3, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r4', 'eWVz', 2, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r5', 'eWVz', 0, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r6', 'eWVz', -1, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r7', 'eWVz', 3, '');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r8', NULL, 8, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r9', 'eWVz', NULL, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r10', 'eWVz', 8, NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', 'bm9ub255ZXM=');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r2', 'bm95ZXM=');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r3', 'eWVz');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r4', 'eWU=');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', '');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r6', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r7', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r8', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r9', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r10', NULL);

--@test: bytes-lpad-rpad - RPAD with all args from record - JSON
CREATE STREAM INPUT (id STRING KEY, subject BYTES, len INT, padding BYTES) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, rpad(subject, len, padding) AS RESULT FROM INPUT;
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r1', 'eWVz', 8, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r2', 'eWVz', 5, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r3', 'eWVz', 3, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r4', 'eWVz', 2, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r5', 'eWVz', 0, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r6', 'eWVz', -1, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r7', 'eWVz', 3, '');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r8', NULL, 3, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r9', 'eWVz', NULL, 'bm8=');
INSERT INTO `INPUT` (ID, subject, len, padding) VALUES ('r10', 'eWVz', -1, NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', 'eWVzbm9ub24=');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r2', 'eWVzbm8=');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r3', 'eWVz');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r4', 'eWU=');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', '');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r6', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r7', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r8', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r9', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r10', NULL);

