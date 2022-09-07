--@test: cube - cube two int columns
CREATE STREAM TEST (ID STRING KEY, col1 INT, col2 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cube_explode(array[col1, col2]) VAL FROM TEST;
INSERT INTO `TEST` (ID, col1, col2) VALUES ('0', 1, 2);
INSERT INTO `TEST` (ID, col1, col2) VALUES ('1', 1, NULL);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES ('0', ARRAY[NULL, NULL]);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES ('0', ARRAY[NULL, 2]);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES ('0', ARRAY[1, NULL]);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES ('0', ARRAY[1, 2]);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES ('1', ARRAY[NULL, NULL]);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES ('1', ARRAY[1, NULL]);

--@test: cube - cube three columns
CREATE STREAM TEST (ID STRING KEY, col1 VARCHAR, col2 VARCHAR, col3 VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cube_explode(array[col1, col2, col3]) VAL FROM TEST;
INSERT INTO `TEST` (ID, col1, col2, col3) VALUES ('0', 'one', 'two', 'three');
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES ('0', ARRAY[NULL, NULL, NULL]);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES ('0', ARRAY[NULL, NULL, 'three']);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES ('0', ARRAY[NULL, 'two', NULL]);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES ('0', ARRAY[NULL, 'two', 'three']);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES ('0', ARRAY['one', NULL, NULL]);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES ('0', ARRAY['one', NULL, 'three']);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES ('0', ARRAY['one', 'two', NULL]);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES ('0', ARRAY['one', 'two', 'three']);

--@test: cube - cube two columns with udf on third
CREATE STREAM TEST (ID STRING KEY, col1 VARCHAR, col2 VARCHAR, col3 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cube_explode(array[col1, col2]) VAL1, ABS(col3) VAL2 FROM TEST;
INSERT INTO `TEST` (ID, col1, col2, col3) VALUES ('0', 'one', 'two', 3);
ASSERT VALUES `OUTPUT` (ID, VAL1, VAL2) VALUES ('0', ARRAY[NULL, NULL], 3.0);
ASSERT VALUES `OUTPUT` (ID, VAL1, VAL2) VALUES ('0', ARRAY[NULL, 'two'], 3.0);
ASSERT VALUES `OUTPUT` (ID, VAL1, VAL2) VALUES ('0', ARRAY['one', NULL], 3.0);
ASSERT VALUES `OUTPUT` (ID, VAL1, VAL2) VALUES ('0', ARRAY['one', 'two'], 3.0);

--@test: cube - cube two columns twice
CREATE STREAM TEST (ID STRING KEY, col1 VARCHAR, col2 VARCHAR, col3 INT, col4 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cube_explode(array[col1, col2]) VAL1, cube_explode(array[col3, col4]) VAL2 FROM TEST;
INSERT INTO `TEST` (ID, col1, col2, col3, col4) VALUES ('0', 'one', 'two', 3, 4);
ASSERT VALUES `OUTPUT` (ID, VAL1, VAL2) VALUES ('0', ARRAY[NULL, NULL], ARRAY[NULL, NULL]);
ASSERT VALUES `OUTPUT` (ID, VAL1, VAL2) VALUES ('0', ARRAY[NULL, 'two'], ARRAY[NULL, 4]);
ASSERT VALUES `OUTPUT` (ID, VAL1, VAL2) VALUES ('0', ARRAY['one', NULL], ARRAY[3, NULL]);
ASSERT VALUES `OUTPUT` (ID, VAL1, VAL2) VALUES ('0', ARRAY['one', 'two'], ARRAY[3, 4]);

