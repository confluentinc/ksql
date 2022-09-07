--@test: transform-map - transform a map
CREATE STREAM TEST (ID BIGINT KEY, VALUE MAP<STRING, ARRAY<INT>>) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT ID, TRANSFORM(VALUE, (x,y) => x + '_test' , (k,v) => ARRAY_MIN(v) + 1) as transform from TEST emit changes;
INSERT INTO `TEST` (ID, value) VALUES (0, MAP('a':=ARRAY[2, 4, 5], 'b':=ARRAY[-1, -2]));
INSERT INTO `TEST` (ID, value) VALUES (1, MAP('q':=ARRAY[]));
INSERT INTO `TEST` (ID, value) VALUES (2, NULL);
ASSERT VALUES `OUTPUT` (ID, TRANSFORM) VALUES (0, MAP('a_test':=3, 'b_test':=-1));
ASSERT VALUES `OUTPUT` (ID, TRANSFORM) VALUES (1, NULL);
ASSERT VALUES `OUTPUT` (ID, TRANSFORM) VALUES (2, NULL);

--@test: transform-map - transformed map with duplicate keys 
CREATE STREAM TEST (ID BIGINT KEY, VALUE MAP<STRING, ARRAY<INTEGER>>) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT ID, TRANSFORM(VALUE, (x,y) => CAST(ARRAY_MAX(y) AS STRING), (x,y) => y) as transform from TEST emit changes;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, MAP('5':=ARRAY[0, 1, 2], '4':=ARRAY[-1, -2]));
INSERT INTO `TEST` (ID, VALUE) VALUES (1, NULL);
INSERT INTO `TEST` (ID, VALUE) VALUES (2, MAP('1':=ARRAY[1, 2], '2':=ARRAY[-1, 2]));
ASSERT VALUES `OUTPUT` (ID, TRANSFORM) VALUES (0, MAP('2':=ARRAY[0, 1, 2], '-1':=ARRAY[-1, -2]));
ASSERT VALUES `OUTPUT` (ID, TRANSFORM) VALUES (1, NULL);
ASSERT VALUES `OUTPUT` (ID, TRANSFORM) VALUES (2, NULL);

--@test: transform-map - capitalize all keys and round values in transformed map
CREATE STREAM TEST (ID BIGINT KEY, VALUE MAP<STRING, DECIMAL(4, 2)>) WITH (kafka_topic='test_topic',value_format='AVRO');
CREATE STREAM OUTPUT as SELECT ID, TRANSFORM(VALUE, (x,y) => UCASE(x), (x,y) => round(y)) AS transform FROM TEST;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, MAP('test':=3.21, 'hello':=4.49));
INSERT INTO `TEST` (ID, VALUE) VALUES (1, MAP('number':=10.50, 'other':=1.01));
ASSERT VALUES `OUTPUT` (ID, TRANSFORM) VALUES (0, MAP('TEST':=3, 'HELLO':=4));
ASSERT VALUES `OUTPUT` (ID, TRANSFORM) VALUES (1, MAP('NUMBER':=11, 'OTHER':=1));

