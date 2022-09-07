--@test: complex-lambda - transform a map with array values
CREATE STREAM TEST (ID BIGINT KEY, VALUE MAP<STRING, ARRAY<INT>>) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT ID, TRANSFORM(TRANSFORM(VALUE, (x,y) => x, (x,y) => FIlTER(y, z => z < 5)), (x,y) => UCASE(x) , (k,v) => ARRAY_MAX(v)) as FILTERED_TRANSFORMED from TEST emit changes;
INSERT INTO `TEST` (ID, value) VALUES (0, MAP('a':=ARRAY[2, NULL, 5, 4], 'b':=ARRAY[-1, -2]));
INSERT INTO `TEST` (ID, value) VALUES (1, MAP('c':=ARRAY[NULL, NULL, -1], 't':=ARRAY[3, 1]));
INSERT INTO `TEST` (ID, value) VALUES (2, MAP('d':=ARRAY[4], 'q':=ARRAY[0, 0]));
ASSERT VALUES `OUTPUT` (ID, FILTERED_TRANSFORMED) VALUES (0, MAP('A':=4, 'B':=-1));
ASSERT VALUES `OUTPUT` (ID, FILTERED_TRANSFORMED) VALUES (1, MAP('C':=-1, 'T':=3));
ASSERT VALUES `OUTPUT` (ID, FILTERED_TRANSFORMED) VALUES (2, MAP('D':=4, 'Q':=0));

--@test: complex-lambda - complex lambda
CREATE STREAM test (ID STRING KEY, MAPPING MAP<STRING, ARRAY<INTEGER>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, TRANSFORM(FILTER(MAPPING, (a, b) => LEN(a) > 2 AND REDUCE(b, 0, (c, d) => c+d) < 20), (X,Y) => LPAD(x, REDUCE(Y, 2, (s, k) => ABS(ABS(k)-s)), 'a'), (X,Y) => REDUCE(ARRAY_UNION(Y, TRANSFORM(Y, z => z*3)), 0, (e, f) => e+f)) AS OUTPUT FROM test;
INSERT INTO `TEST` (ID, MAPPING) VALUES ('one', MAP('a':=ARRAY[2, 4, 5], 'bcd':=ARRAY[-5, 7]));
INSERT INTO `TEST` (ID, MAPPING) VALUES ('two', MAP('hello':=ARRAY[200, 4, 5], 'hey':=ARRAY[14, -3, -15, 3], 'wow':=ARRAY[2, 3, 4]));
INSERT INTO `TEST` (ID, MAPPING) VALUES ('three', MAP('a':=NULL, 'bcdefg':=ARRAY[-15, 72]));
INSERT INTO `TEST` (ID, MAPPING) VALUES ('four', NULL);
ASSERT VALUES `OUTPUT` (ID, OUTPUT) VALUES ('one', MAP('abcd':=8));
ASSERT VALUES `OUTPUT` (ID, OUTPUT) VALUES ('two', MAP('hey':=-4, 'w':=36));
ASSERT VALUES `OUTPUT` (ID, OUTPUT) VALUES ('three', MAP());
ASSERT VALUES `OUTPUT` (ID, OUTPUT) VALUES ('four', NULL);

--@test: complex-lambda - reduce an array of maps
CREATE STREAM test (ID STRING KEY, arraying ARRAY<MAP<STRING, INTEGER>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, reduce(arraying, 5, (s, a) => s + REDUCE(TRANSFORM(FILTER(a, (x, y) => len(x) > 5 AND y % 2 != 0), (e, f) => concat(e, 'leah'), (g, h) => h + len(g)), 0, (s2,r, b) => s2+2*b)) AS OUTPUT FROM test;
INSERT INTO `TEST` (ID, arraying) VALUES ('one', ARRAY[MAP('to be or not to be':=15), MAP('hello':=25)]);
INSERT INTO `TEST` (ID, arraying) VALUES ('two', ARRAY[MAP('goodmorning':=23, 'gn':=12), MAP('woooooow':=9)]);
INSERT INTO `TEST` (ID, arraying) VALUES ('three', ARRAY[MAP('a':=NULL, 'bcdefg':=4)]);
INSERT INTO `TEST` (ID, arraying) VALUES ('four', NULL);
ASSERT VALUES `OUTPUT` (ID, OUTPUT) VALUES ('one', 71);
ASSERT VALUES `OUTPUT` (ID, OUTPUT) VALUES ('two', 107);
ASSERT VALUES `OUTPUT` (ID, OUTPUT) VALUES ('three', 5);
ASSERT VALUES `OUTPUT` (ID, OUTPUT) VALUES ('four', 5);

