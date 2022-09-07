--@test: map - string map
CREATE STREAM INPUT (ID STRING KEY, A_MAP MAP<STRING, INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, A_MAP['expected'], A_MAP['missing'] FROM INPUT;
INSERT INTO `INPUT` (A_MAP) VALUES (MAP('expected':=10));
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES (10, NULL);

--@test: map - map value as UDF param
CREATE STREAM INPUT (ID STRING KEY, col11 MAP<STRING, STRING>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, EXTRACTJSONFIELD(col11['address'], '$.city') FROM INPUT;
INSERT INTO `INPUT` (col11) VALUES (MAP('address':='{"city": "London"}'));
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES ('London');

--@test: map - map_keys
CREATE STREAM INPUT (id STRING KEY, a_map MAP<STRING, INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_sort(map_keys(a_map)) as keys FROM INPUT;
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r1', MAP('foo':=10, 'bar':=20, 'baz':=30));
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r2', MAP('foo':=10, 'bar':=NULL));
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r3', MAP());
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r4', NULL);
ASSERT VALUES `OUTPUT` (ID, KEYS) VALUES ('r1', ARRAY['bar', 'baz', 'foo']);
ASSERT VALUES `OUTPUT` (ID, KEYS) VALUES ('r2', ARRAY['bar', 'foo']);
ASSERT VALUES `OUTPUT` (ID, KEYS) VALUES ('r3', ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, KEYS) VALUES ('r4', NULL);

--@test: map - map_keys with non-primitive values
CREATE STREAM INPUT (id STRING KEY, a_map MAP<STRING, ARRAY<INT>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_sort(map_keys(a_map)) as keys FROM INPUT;
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r1', MAP('foo':=ARRAY[1, 2, 3], 'bar':=ARRAY[10, 20, 30], 'baz':=ARRAY[100, 200, 300]));
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r2', MAP('foo':=ARRAY[1, 2, 3], 'bar':=NULL));
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r3', MAP());
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r4', NULL);
ASSERT VALUES `OUTPUT` (ID, KEYS) VALUES ('r1', ARRAY['bar', 'baz', 'foo']);
ASSERT VALUES `OUTPUT` (ID, KEYS) VALUES ('r2', ARRAY['bar', 'foo']);
ASSERT VALUES `OUTPUT` (ID, KEYS) VALUES ('r3', ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, KEYS) VALUES ('r4', NULL);

--@test: map - map_values
CREATE STREAM INPUT (id STRING KEY, a_map MAP<STRING, INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_sort(map_values(a_map)) as vals FROM INPUT;
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r1', MAP('foo':=10, 'bar':=20, 'baz':=30));
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r2', MAP('foo':=10, 'bar':=NULL));
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r3', MAP());
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r4', NULL);
ASSERT VALUES `OUTPUT` (ID, VALS) VALUES ('r1', ARRAY[10, 20, 30]);
ASSERT VALUES `OUTPUT` (ID, VALS) VALUES ('r2', ARRAY[10, NULL]);
ASSERT VALUES `OUTPUT` (ID, VALS) VALUES ('r3', ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, VALS) VALUES ('r4', NULL);

--@test: map - map_values with non-primitive values
CREATE STREAM INPUT (id STRING KEY, a_map MAP<STRING, ARRAY<INT>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, map_values(a_map) as vals FROM INPUT;
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r1', MAP('foo':=ARRAY[1, 2, 3]));
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r2', MAP('foo':=NULL, 'bar':=NULL));
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r3', MAP());
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r4', NULL);
INSERT INTO `INPUT` (ID, A_MAP) VALUES ('r5', MAP('foo':=ARRAY[NULL], 'bar':=ARRAY[NULL]));
ASSERT VALUES `OUTPUT` (ID, VALS) VALUES ('r1', ARRAY[ARRAY[1, 2, 3]]);
ASSERT VALUES `OUTPUT` (ID, VALS) VALUES ('r2', ARRAY[NULL, NULL]);
ASSERT VALUES `OUTPUT` (ID, VALS) VALUES ('r3', ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, VALS) VALUES ('r4', NULL);
ASSERT VALUES `OUTPUT` (ID, VALS) VALUES ('r5', ARRAY[ARRAY[NULL], ARRAY[NULL]]);

--@test: map - map_union
CREATE STREAM INPUT (id STRING KEY, map_1 MAP<STRING, INT>, map_2 MAP<STRING, INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, map_union(map_1, map_2) as combined FROM INPUT;
INSERT INTO `INPUT` (ID, MAP_1, MAP_2) VALUES ('r1', MAP('foo':=10, 'bar':=20, 'baz':=30), MAP('foo':=99, 'apple':=-1));
INSERT INTO `INPUT` (ID, MAP_1, MAP_2) VALUES ('r2', MAP('foo':=10, 'bar':=20), MAP('foo':=NULL));
INSERT INTO `INPUT` (ID, MAP_1, MAP_2) VALUES ('r3', MAP('foo':=10, 'bar':=20), MAP());
INSERT INTO `INPUT` (ID, MAP_1, MAP_2) VALUES ('r4', MAP(), MAP());
INSERT INTO `INPUT` (ID, MAP_1, MAP_2) VALUES ('r5', NULL, MAP('foo':=NULL));
ASSERT VALUES `OUTPUT` (ID, COMBINED) VALUES ('r1', MAP('foo':=99, 'bar':=20, 'baz':=30, 'apple':=-1));
ASSERT VALUES `OUTPUT` (ID, COMBINED) VALUES ('r2', MAP('foo':=NULL, 'bar':=20));
ASSERT VALUES `OUTPUT` (ID, COMBINED) VALUES ('r3', MAP('foo':=10, 'bar':=20));
ASSERT VALUES `OUTPUT` (ID, COMBINED) VALUES ('r4', MAP());
ASSERT VALUES `OUTPUT` (ID, COMBINED) VALUES ('r5', MAP('foo':=NULL));

--@test: map - Output map with an error
CREATE STREAM test (K STRING KEY, val VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, MAP('boo':=stringtodate(val, 'yyyyMMdd'), 'shoe':=3) AS VALUE FROM test;
INSERT INTO `TEST` (K, val, ROWTIME) VALUES ('1', 'foo', 0);
ASSERT VALUES `OUTPUT` (K, VALUE, ROWTIME) VALUES ('1', MAP('boo':=NULL, 'shoe':=3), 0);

