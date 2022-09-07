--@test: split-to-map - split_to_map
CREATE STREAM INPUT (id STRING KEY, input STRING, entryDelim STRING, kvDelim STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, split_to_map(input, entryDelim, kvDelim) as result FROM INPUT;
INSERT INTO `INPUT` (ID, input, entryDelim, kvDelim) VALUES ('r1', 'apple:green/banana:yellow/cherry:red', '/', ':');
INSERT INTO `INPUT` (ID, input, entryDelim, kvDelim) VALUES ('r2', 'apple:green/banana:yellow/cherry:red', '&&', ':');
INSERT INTO `INPUT` (ID, input, entryDelim, kvDelim) VALUES ('r3', 'apple:green//banana:yellow//cherry:red/', '/', ':');
INSERT INTO `INPUT` (ID, input, entryDelim, kvDelim) VALUES ('r4', 'apple:green/banana:yellow/cherry:red', '/', '=');
INSERT INTO `INPUT` (ID, input, entryDelim, kvDelim) VALUES ('r5', 'apple:green/banana:yellow/cherry:red', '', ':');
INSERT INTO `INPUT` (ID, input, entryDelim, kvDelim) VALUES ('r6', 'apple:green/banana:yellow/cherry:red', '/', '');
INSERT INTO `INPUT` (ID, input, entryDelim, kvDelim) VALUES ('r7', 'apple:green/banana:yellow/cherry:red', NULL, ':');
INSERT INTO `INPUT` (ID, input, entryDelim, kvDelim) VALUES ('r8', 'apple:green/banana:yellow/cherry:red', '/', NULL);
INSERT INTO `INPUT` (ID, input, entryDelim, kvDelim) VALUES ('r9', '', '/', ':');
INSERT INTO `INPUT` (ID, input, entryDelim, kvDelim) VALUES ('r10', NULL, '/', ':');
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', MAP('apple':='green', 'banana':='yellow', 'cherry':='red'));
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r2', MAP('apple':='green/banana'));
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r3', MAP('apple':='green', 'banana':='yellow', 'cherry':='red'));
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r4', MAP());
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r6', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r7', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r8', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r9', MAP());
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r10', NULL);

