--@test: filter - apply lambda filter to int array
CREATE STREAM test (ID STRING KEY, numbers ARRAY<INTEGER>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, filter(numbers, x => x > 5) AS c FROM test;
INSERT INTO `TEST` (ID, numbers) VALUES ('one', ARRAY[3, 6, 2, 10]);
INSERT INTO `TEST` (ID, numbers) VALUES ('two', ARRAY[5, NULL]);
INSERT INTO `TEST` (ID, numbers) VALUES ('three', NULL);
ASSERT VALUES `OUTPUT` (ID, C) VALUES ('one', ARRAY[6, 10]);
ASSERT VALUES `OUTPUT` (ID, C) VALUES ('two', ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, C) VALUES ('three', NULL);

--@test: filter - apply lambda filter with udf to array
CREATE STREAM TEST (ID BIGINT KEY, VALUE ARRAY<DECIMAL(4,2)>) WITH (kafka_topic='test_topic',value_format='AVRO');
CREATE STREAM OUTPUT as SELECT ID, filter(VALUE, x => round(x) >= 10) AS LAMBDA FROM TEST;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, ARRAY[9.22, 9.56, 10.63, 7.32]);
INSERT INTO `TEST` (ID, VALUE) VALUES (1, ARRAY[10.55, NULL, 10.45]);
INSERT INTO `TEST` (ID, VALUE) VALUES (2, NULL);
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES (0, ARRAY[9.56, 10.63]);
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES (1, ARRAY[10.55, 10.45]);
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES (2, NULL);

--@test: filter - apply lambda filter to string array
CREATE STREAM TEST (ID BIGINT KEY, VALUE ARRAY<STRING>) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT ID, filter(VALUE, x => instr(x, 'ya') > 0) as LAMBDA from TEST emit changes;
INSERT INTO `TEST` (ID, value) VALUES (4, ARRAY['suuure', 'ya i see it', NULL]);
INSERT INTO `TEST` (ID, value) VALUES (5, ARRAY['whatever ya want', 'nothing here']);
INSERT INTO `TEST` (ID, value) VALUES (6, NULL);
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES (4, ARRAY['ya i see it']);
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES (5, ARRAY['whatever ya want']);
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES (6, NULL);

--@test: filter - filter a nested map
CREATE STREAM TEST (ID BIGINT KEY, VALUE MAP<STRING, ARRAY<INTEGER>>) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT ID, filter(VALUE, (x,y) => instr(x, 'e') > 0 AND ARRAY_MIN(y) < 12) as LAMBDA from TEST emit changes;
INSERT INTO `TEST` (ID, value) VALUES (7, MAP('yes':=ARRAY[2, 14, 25], 'no':=ARRAY[-4, 35]));
INSERT INTO `TEST` (ID, value) VALUES (8, MAP('sure':=ARRAY[234, 245, 23], 'leah':=ARRAY[-4, 35]));
INSERT INTO `TEST` (ID, value) VALUES (9, MAP('nope':=ARRAY[-45, 14, 25], 'stvn':=ARRAY[-4, 35]));
INSERT INTO `TEST` (ID, value) VALUES (10, MAP('okey':=ARRAY[]));
INSERT INTO `TEST` (ID, value) VALUES (11, NULL);
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES (7, MAP('yes':=ARRAY[2, 14, 25]));
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES (8, MAP('leah':=ARRAY[-4, 35]));
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES (9, MAP('nope':=ARRAY[-45, 14, 25]));
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES (10, MAP());
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES (11, NULL);

--@test: filter - apply filter lambda function to int map
CREATE STREAM test (ID STRING KEY, map MAP<STRING, INTEGER>) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT AS SELECT ID, filter(map, (x,y) => len(x) * 5 > 15 AND y % 5 = 0) AS lambda FROM test;
INSERT INTO `TEST` (ID, map) VALUES ('four', MAP('a':=15, 'abc':=7));
INSERT INTO `TEST` (ID, map) VALUES ('five', MAP('ab':=6, 'abcd':=15));
INSERT INTO `TEST` (ID, map) VALUES ('six', MAP('abcd':=NULL, 'abcde':=25));
INSERT INTO `TEST` (ID, map) VALUES ('seven', MAP('null':=15, 'abcde':=25));
INSERT INTO `TEST` (ID, map) VALUES ('eight', NULL);
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES ('four', MAP());
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES ('five', MAP('abcd':=15));
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES ('six', NULL);
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES ('seven', MAP('null':=15, 'abcde':=25));
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES ('eight', NULL);

