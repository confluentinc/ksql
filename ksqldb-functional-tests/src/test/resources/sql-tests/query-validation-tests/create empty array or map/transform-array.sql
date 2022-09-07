--@test: transform-array - apply transform lambda function to array
CREATE STREAM test (ID STRING KEY, numbers ARRAY<INTEGER>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, TRANSFORM(numbers, x => x + 5) AS c FROM test;
INSERT INTO `TEST` (ID, numbers) VALUES ('one', ARRAY[3, 6]);
INSERT INTO `TEST` (ID, numbers) VALUES ('two', ARRAY[5, NULL]);
INSERT INTO `TEST` (ID, numbers) VALUES ('three', NULL);
ASSERT VALUES `OUTPUT` (ID, C) VALUES ('one', ARRAY[8, 11]);
ASSERT VALUES `OUTPUT` (ID, C) VALUES ('two', NULL);
ASSERT VALUES `OUTPUT` (ID, C) VALUES ('three', NULL);

--@test: transform-array - capitalize all array elements
CREATE STREAM TEST (ID BIGINT KEY, VALUE ARRAY<string>) WITH (kafka_topic='test_topic',value_format='AVRO');
CREATE STREAM OUTPUT as SELECT ID, TRANSFORM(VALUE, x => UCASE(x)) AS LAMBDA FROM TEST;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, ARRAY['hello', 'these', 'are', 'my', 'strings']);
INSERT INTO `TEST` (ID, VALUE) VALUES (1, ARRAY['check', NULL, 'null']);
INSERT INTO `TEST` (ID, VALUE) VALUES (2, ARRAY['ksqldb', 'kafka', 'streams']);
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES (0, ARRAY['HELLO', 'THESE', 'ARE', 'MY', 'STRINGS']);
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES (1, ARRAY['CHECK', NULL, 'NULL']);
ASSERT VALUES `OUTPUT` (ID, LAMBDA) VALUES (2, ARRAY['KSQLDB', 'KAFKA', 'STREAMS']);

--@test: transform-array - case check on all array elements - input: double, output: string
CREATE STREAM TEST (ID BIGINT KEY, VALUE ARRAY<double>) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT ID, TRANSFORM(VALUE, x => CASE WHEN x > 10 THEN 'above 10' ELSE 'TOO LOW' END) as SUM from TEST emit changes;
INSERT INTO `TEST` (ID, value) VALUES (0, ARRAY[2.32, 12123, 3.123, 4.45]);
INSERT INTO `TEST` (ID, value) VALUES (5, ARRAY[11, 13, NULL]);
INSERT INTO `TEST` (ID, value) VALUES (100, ARRAY[10, 5, 4]);
INSERT INTO `TEST` (ID, value) VALUES (110, ARRAY[2, 3, 100]);
ASSERT VALUES `OUTPUT` (ID, SUM) VALUES (0, ARRAY['TOO LOW', 'above 10', 'TOO LOW', 'TOO LOW']);
ASSERT VALUES `OUTPUT` (ID, SUM) VALUES (5, ARRAY['above 10', 'above 10', 'TOO LOW']);
ASSERT VALUES `OUTPUT` (ID, SUM) VALUES (100, ARRAY['TOO LOW', 'TOO LOW', 'TOO LOW']);
ASSERT VALUES `OUTPUT` (ID, SUM) VALUES (110, ARRAY['TOO LOW', 'TOO LOW', 'above 10']);

--@test: transform-array - decimal absolute on all array elements
CREATE STREAM TEST (ID BIGINT KEY, VALUE ARRAY<DECIMAL(3, 2)>) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT ID, TRANSFORM(VALUE, x => abs(x)) as abs from TEST emit changes;
INSERT INTO `TEST` (ID, value) VALUES (0, ARRAY[-2.45, 3.67, 1.23]);
INSERT INTO `TEST` (ID, value) VALUES (5, ARRAY[-7.45, -1.34, NULL]);
INSERT INTO `TEST` (ID, value) VALUES (100, ARRAY[1.45, 5.68, -4.67]);
INSERT INTO `TEST` (ID, value) VALUES (110, NULL);
ASSERT VALUES `OUTPUT` (ID, ABS) VALUES (0, ARRAY[2.45, 3.67, 1.23]);
ASSERT VALUES `OUTPUT` (ID, ABS) VALUES (5, ARRAY[7.45, 1.34, NULL]);
ASSERT VALUES `OUTPUT` (ID, ABS) VALUES (100, ARRAY[1.45, 5.68, 4.67]);
ASSERT VALUES `OUTPUT` (ID, ABS) VALUES (110, NULL);

--@test: transform-array - array max on array of arrays
CREATE STREAM TEST (ID BIGINT KEY, VALUE ARRAY<ARRAY<INTEGER>>) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT ID, TRANSFORM(VALUE, x => array_max(x)) as max from TEST emit changes;
INSERT INTO `TEST` (ID, value) VALUES (0, ARRAY[ARRAY[5, 7, 1], ARRAY[3, 6, 1]]);
INSERT INTO `TEST` (ID, value) VALUES (5, ARRAY[ARRAY[123, 452, 451, NULL], ARRAY[532, 123, 78]]);
INSERT INTO `TEST` (ID, value) VALUES (100, ARRAY[ARRAY[90, 341, 2], ARRAY[234, 123, 865]]);
INSERT INTO `TEST` (ID, value) VALUES (110, NULL);
ASSERT VALUES `OUTPUT` (ID, MAX) VALUES (0, ARRAY[7, 6]);
ASSERT VALUES `OUTPUT` (ID, MAX) VALUES (5, ARRAY[452, 532]);
ASSERT VALUES `OUTPUT` (ID, MAX) VALUES (100, ARRAY[341, 865]);
ASSERT VALUES `OUTPUT` (ID, MAX) VALUES (110, NULL);

--@test: transform-array - apply transform lambda function to array of timestamps
CREATE STREAM test (ID STRING KEY, times ARRAY<TIMESTAMP>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, TRANSFORM(times, x => timestampadd(milliseconds, 5, x)) AS c FROM test;
INSERT INTO `TEST` (ID, times) VALUES ('one', ARRAY['1970-01-01T00:00:00.003', '1970-01-01T00:00:00.006', NULL]);
ASSERT VALUES `OUTPUT` (ID, C) VALUES ('one', ARRAY['1970-01-01T00:00:00.008', '1970-01-01T00:00:00.011', NULL]);

--@test: transform-array - transform array of struct
CREATE STREAM TEST (ID BIGINT KEY, VALUE ARRAY<STRUCT<c VARCHAR, d INT>>) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT ID, TRANSFORM(VALUE, x => STRUCT(Q := x->d + 5, P := x->c + 'h')) as transformed_struct from TEST emit changes;
INSERT INTO `TEST` (ID, value) VALUES (0, ARRAY[STRUCT(c:='ste', d:=2), STRUCT(c:='q', d:=0)]);
INSERT INTO `TEST` (ID, value) VALUES (1, ARRAY[STRUCT(c:='lea', d:=1), STRUCT(c:='pro', d:=-1)]);
INSERT INTO `TEST` (ID, value) VALUES (2, NULL);
ASSERT VALUES `OUTPUT` (ID, TRANSFORMED_STRUCT) VALUES (0, ARRAY[STRUCT(P:='steh', Q:=7), STRUCT(P:='qh', Q:=5)]);
ASSERT VALUES `OUTPUT` (ID, TRANSFORMED_STRUCT) VALUES (1, ARRAY[STRUCT(P:='leah', Q:=6), STRUCT(P:='proh', Q:=4)]);
ASSERT VALUES `OUTPUT` (ID, TRANSFORMED_STRUCT) VALUES (2, NULL);

--@test: transform-array - transform a list into compatible mismatching struct elements
CREATE STREAM TEST (a INT, BIGINT_ARRAY Array<BIGINT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT TRANSFORM(BIGINT_ARRAY, x => ARRAY[null, struct(x := a, y := x), struct(y := a, z:= x)]) as transformed FROM TEST;
INSERT INTO `TEST` (a, BIGINT_ARRAY) VALUES (30, ARRAY[40, 50]);
ASSERT VALUES `OUTPUT` (TRANSFORMED) VALUES (ARRAY[ARRAY[NULL, STRUCT(X:=30, Y:=40, Z:=NULL), STRUCT(X:=NULL, Y:=30, Z:=40)], ARRAY[NULL, STRUCT(X:=30, Y:=50, Z:=NULL), STRUCT(X:=NULL, Y:=30, Z:=50)]]);

