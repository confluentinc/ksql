--@test: reduce-map - apply reduce lambda function to map
CREATE STREAM test (ID STRING KEY, map MAP<STRING, INTEGER>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, REDUCE(map, 0, (s, k, v) => CASE WHEN LEN(k) > 3 THEN  s + v ELSE s - v END) AS reduce FROM test;
INSERT INTO `TEST` (ID, map) VALUES ('zero', MAP('123':=3, '12':=7, '1234':=2));
INSERT INTO `TEST` (ID, map) VALUES ('one', MAP('1':=1, 'ttttt':=NULL, '':=3));
INSERT INTO `TEST` (ID, map) VALUES ('two', NULL);
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('zero', -8);
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('one', NULL);
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('two', 0);

--@test: reduce-map - reduce map with null initial state
CREATE STREAM test (ID STRING KEY, map MAP<STRING, INTEGER>, state BIGINT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, REDUCE(map, state, (s,x,y) => y + s) AS reduce FROM test;
INSERT INTO `TEST` (ID, map, state) VALUES ('one', MAP('test1':=6, 'test2':=7), NULL);
INSERT INTO `TEST` (ID, map, state) VALUES ('two', NULL, NULL);
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('one', NULL);
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('two', NULL);

--@test: reduce-map - reduce map with double
CREATE STREAM test (ID STRING KEY, map MAP<STRING, DOUBLE>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, REDUCE(map, CAST(3.2 AS DOUBLE), (s,x,y) => CASE WHEN instr(x, 'ye') > 0 THEN ceil(y * s) ELSE floor(s * y) END) AS reduce FROM test;
INSERT INTO `TEST` (ID, map) VALUES ('zero', MAP('yes, thanks':=3.5, 'nope':=7.3, 'sure, yeah':=2.1));
INSERT INTO `TEST` (ID, map) VALUES ('one', MAP('yeowza':=1.8, 'nah':=NULL, '':=3.2));
INSERT INTO `TEST` (ID, map) VALUES ('two', NULL);
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('zero', 189.0);
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('one', NULL);
ASSERT VALUES `OUTPUT` (ID, REDUCE) VALUES ('two', 3.2);

