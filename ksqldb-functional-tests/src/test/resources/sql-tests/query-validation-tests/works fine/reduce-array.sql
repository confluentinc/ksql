--@test: reduce-array - reduce an array
CREATE STREAM test (ID STRING KEY, numbers ARRAY<INTEGER>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, REDUCE(numbers, 2, (s, x) => s + x) AS reduce FROM test;
INSERT INTO `TEST` (ID, numbers) VALUES ('one', ARRAY[3, 6]);
INSERT INTO `TEST` (ID, numbers) VALUES ('two', ARRAY[5, NULL]);
INSERT INTO `TEST` (ID, numbers) VALUES ('three', NULL);
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('one', 11);
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('two', NULL);
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('three', 2);

--@test: reduce-array - reduce an array with null initial state
CREATE STREAM test (ID STRING KEY, numbers ARRAY<INTEGER>, state BIGINT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, REDUCE(numbers, state, (s, x) => s + x) AS reduce FROM test;
INSERT INTO `TEST` (ID, numbers, state) VALUES ('one', ARRAY[1, 2], NULL);
INSERT INTO `TEST` (ID, numbers, state) VALUES ('two', NULL, NULL);
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('one', NULL);
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('two', NULL);

--@test: reduce-array - reduce a string array
CREATE STREAM test (ID STRING KEY, numbers ARRAY<STRING>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, REDUCE(numbers, '', (state, x) => concat(state, x)) AS reduce FROM test;
INSERT INTO `TEST` (ID, numbers) VALUES ('one', ARRAY['blah', 'blah', 'blaaah']);
INSERT INTO `TEST` (ID, numbers) VALUES ('two', ARRAY['to be or', NULL]);
INSERT INTO `TEST` (ID, numbers) VALUES ('three', NULL);
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('one', 'blahblahblaaah');
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('two', 'to be or');
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('three', '');

