--@test: array-set-functions - array_distinct with literals
CREATE STREAM INPUT (id STRING KEY, dummy INTEGER) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_distinct(array['foo', 'bar', 'foo']) as a1 FROM INPUT;
INSERT INTO `INPUT` (ID, dummy) VALUES ('r1', 0);
INSERT INTO `INPUT` (ID, dummy) VALUES ('r2', 0);
ASSERT VALUES `OUTPUT` (ID, A1) VALUES ('r1', ARRAY['foo', 'bar']);
ASSERT VALUES `OUTPUT` (ID, A1) VALUES ('r2', ARRAY['foo', 'bar']);

--@test: array-set-functions - array_distinct with primitive types
CREATE STREAM INPUT (id STRING KEY, bools ARRAY<BOOLEAN>, ints ARRAY<INT>, bigints ARRAY<BIGINT>, doubles ARRAY<DOUBLE>, strings ARRAY<STRING>, decimals ARRAY<DECIMAL(2,1)>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_distinct(bools) as bools_dist, array_distinct(ints) as ints_dist, array_distinct(bigints) as bigints_dist, array_distinct(doubles) as doubles_dist, array_distinct(strings) as strings_dist, array_distinct(decimals) as decimals_dist FROM INPUT;
INSERT INTO `INPUT` (ID, bools, ints, bigints, doubles, strings, decimals) VALUES ('r1', ARRAY[false, true, false], ARRAY[0, 0, 1, 0, -1], ARRAY[345, -123, 345], ARRAY[0.0, 0.2, -12345.678, 0.2], ARRAY['foo', 'bar', 'foo'], ARRAY[1.0, -0.2, 1.0, -9.9]);
INSERT INTO `INPUT` (ID, bools, ints, bigints, doubles, strings, decimals) VALUES ('r2', ARRAY[NULL, false, true], ARRAY[0, NULL, 1, 0, -1], ARRAY[NULL, -123], ARRAY[0.3, -12345.678, NULL, 0.3], ARRAY['foo', 'Food', NULL, 'food'], ARRAY[1.0, 1.1, 1.1, -0.2, NULL, 1.0]);
INSERT INTO `INPUT` (ID, bools, ints, bigints, doubles, strings, decimals) VALUES ('r3', ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[]);
INSERT INTO `INPUT` (ID, bools, ints, bigints, doubles, strings, decimals) VALUES ('r4', NULL, NULL, NULL, NULL, NULL, NULL);
ASSERT VALUES `OUTPUT` (ID, BOOLS_DIST, INTS_DIST, BIGINTS_DIST, DOUBLES_DIST, STRINGS_DIST, DECIMALS_DIST) VALUES ('r1', ARRAY[false, true], ARRAY[0, 1, -1], ARRAY[345, -123], ARRAY[0.0, 0.2, -12345.678], ARRAY['foo', 'bar'], ARRAY[1.0, -0.2, -9.9]);
ASSERT VALUES `OUTPUT` (ID, BOOLS_DIST, INTS_DIST, BIGINTS_DIST, DOUBLES_DIST, STRINGS_DIST, DECIMALS_DIST) VALUES ('r2', ARRAY[NULL, false, true], ARRAY[0, NULL, 1, -1], ARRAY[NULL, -123], ARRAY[0.3, -12345.678, NULL], ARRAY['foo', 'Food', NULL, 'food'], ARRAY[1.0, 1.1, -0.2, NULL]);
ASSERT VALUES `OUTPUT` (ID, BOOLS_DIST, INTS_DIST, BIGINTS_DIST, DOUBLES_DIST, STRINGS_DIST, DECIMALS_DIST) VALUES ('r3', ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, BOOLS_DIST, INTS_DIST, BIGINTS_DIST, DOUBLES_DIST, STRINGS_DIST, DECIMALS_DIST) VALUES ('r4', NULL, NULL, NULL, NULL, NULL, NULL);

--@test: array-set-functions - array_distinct with complex types
CREATE STREAM INPUT (id STRING KEY, lists ARRAY<ARRAY<STRING>>, maps ARRAY<MAP<STRING,INT>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_distinct(lists) as lists_dist, array_distinct(maps) as maps_dist FROM INPUT;
INSERT INTO `INPUT` (ID, lists, maps) VALUES ('r1', ARRAY[ARRAY['foo', 'bar', 'foo'], ARRAY['foo', 'bar', 'foo'], ARRAY['foo']], ARRAY[MAP('apple':=1, 'banana':=2), MAP('apple':=3, 'banana':=4), MAP('apple':=1, 'banana':=2)]);
INSERT INTO `INPUT` (ID, lists, maps) VALUES ('r2', ARRAY[ARRAY['foo', NULL], ARRAY['foo', 'bar'], ARRAY['foo']], ARRAY[MAP('apple':=NULL, 'banana':=2), MAP('apple':=1, 'banana':=2), MAP('apple':=1, 'banana':=2)]);
INSERT INTO `INPUT` (ID, lists, maps) VALUES ('r3', ARRAY[NULL, ARRAY['foo']], ARRAY[MAP('apple':=1, 'banana':=2), NULL]);
INSERT INTO `INPUT` (ID, lists, maps) VALUES ('r4', ARRAY[], ARRAY[]);
INSERT INTO `INPUT` (ID, lists, maps) VALUES ('r5', NULL, NULL);
ASSERT VALUES `OUTPUT` (ID, LISTS_DIST, MAPS_DIST) VALUES ('r1', ARRAY[ARRAY['foo', 'bar', 'foo'], ARRAY['foo']], ARRAY[MAP('apple':=1, 'banana':=2), MAP('apple':=3, 'banana':=4)]);
ASSERT VALUES `OUTPUT` (ID, LISTS_DIST, MAPS_DIST) VALUES ('r2', ARRAY[ARRAY['foo', NULL], ARRAY['foo', 'bar'], ARRAY['foo']], ARRAY[MAP('apple':=NULL, 'banana':=2), MAP('apple':=1, 'banana':=2)]);
ASSERT VALUES `OUTPUT` (ID, LISTS_DIST, MAPS_DIST) VALUES ('r3', ARRAY[NULL, ARRAY['foo']], ARRAY[MAP('apple':=1, 'banana':=2), NULL]);
ASSERT VALUES `OUTPUT` (ID, LISTS_DIST, MAPS_DIST) VALUES ('r4', ARRAY[], ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, LISTS_DIST, MAPS_DIST) VALUES ('r5', NULL, NULL);

--@test: array-set-functions - array_except with literals
CREATE STREAM INPUT (id STRING KEY, dummy INTEGER) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_except(array['foo', 'bar', 'foo'], array['bar', 'baz']) as a1 FROM INPUT;
INSERT INTO `INPUT` (ID, dummy) VALUES ('r1', 0);
INSERT INTO `INPUT` (ID, dummy) VALUES ('r2', 0);
ASSERT VALUES `OUTPUT` (ID, A1) VALUES ('r1', ARRAY['foo']);
ASSERT VALUES `OUTPUT` (ID, A1) VALUES ('r2', ARRAY['foo']);

--@test: array-set-functions - array_except with primitive type
CREATE STREAM INPUT (id STRING KEY, ints ARRAY<INT>, int_exceptions ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_except(ints, int_exceptions) as result FROM INPUT;
INSERT INTO `INPUT` (ID, ints, int_exceptions) VALUES ('r1', ARRAY[0, 0, 1, 0, -1], ARRAY[1, -2]);
INSERT INTO `INPUT` (ID, ints, int_exceptions) VALUES ('r2', ARRAY[0, 0, 1, 0, -1], ARRAY[1, -1, 0]);
INSERT INTO `INPUT` (ID, ints, int_exceptions) VALUES ('r3', ARRAY[], ARRAY[1, -2]);
INSERT INTO `INPUT` (ID, ints, int_exceptions) VALUES ('r4', ARRAY[0, 0, 1, 0, -1], ARRAY[]);
INSERT INTO `INPUT` (ID, ints, int_exceptions) VALUES ('r5', NULL, ARRAY[1, -2]);
INSERT INTO `INPUT` (ID, ints, int_exceptions) VALUES ('r6', ARRAY[0, 0, 1, 0, -1], NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', ARRAY[0, -1]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r2', ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r3', ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r4', ARRAY[0, 1, -1]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r6', NULL);

--@test: array-set-functions - array_intersect with literals
CREATE STREAM INPUT (id STRING KEY, dummy INTEGER) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_intersect(array['foo', 'bar', 'foo'], array['foo', 'baz']) as a1 FROM INPUT;
INSERT INTO `INPUT` (ID, dummy) VALUES ('r1', 0);
INSERT INTO `INPUT` (ID, dummy) VALUES ('r2', 0);
ASSERT VALUES `OUTPUT` (ID, A1) VALUES ('r1', ARRAY['foo']);
ASSERT VALUES `OUTPUT` (ID, A1) VALUES ('r2', ARRAY['foo']);

--@test: array-set-functions - array_intersect with primitive type
CREATE STREAM INPUT (id STRING KEY, arr1 ARRAY<INT>, arr2 ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_intersect(arr1, arr2) as result FROM INPUT;
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r1', ARRAY[0, 0, 1, 0, -1], ARRAY[1, -2, 0]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r2', ARRAY[0, 0, 1, 0, -1], ARRAY[3, 4]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r3', ARRAY[], ARRAY[1, -2]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r4', ARRAY[0, 0, 1, 0, -1], ARRAY[]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r5', NULL, ARRAY[1, -2]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r6', ARRAY[0, 0, 1, 0, -1], NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', ARRAY[0, 1]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r2', ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r3', ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r4', ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r6', NULL);

--@test: array-set-functions - array_union with literals
CREATE STREAM INPUT (id STRING KEY, dummy INTEGER) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_union(array['foo', 'bar', 'foo'], array['foo', 'baz']) as a1 FROM INPUT;
INSERT INTO `INPUT` (ID, dummy) VALUES ('r1', 0);
INSERT INTO `INPUT` (ID, dummy) VALUES ('r2', 0);
ASSERT VALUES `OUTPUT` (ID, A1) VALUES ('r1', ARRAY['foo', 'bar', 'baz']);
ASSERT VALUES `OUTPUT` (ID, A1) VALUES ('r2', ARRAY['foo', 'bar', 'baz']);

--@test: array-set-functions - array_union with primitive type
CREATE STREAM INPUT (id STRING KEY, arr1 ARRAY<INT>, arr2 ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_union(arr1, arr2) as result FROM INPUT;
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r1', ARRAY[0, 0, 1, 0, -1], ARRAY[1, -2, 0]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r2', ARRAY[0, 0, 1, 0, -1], ARRAY[3, 4]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r3', ARRAY[], ARRAY[1, -2]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r4', ARRAY[0, 0, 1, 0, -1], ARRAY[]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r5', NULL, ARRAY[1, -2]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r6', ARRAY[0, 0, 1, 0, -1], NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', ARRAY[0, 1, -1, -2]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r2', ARRAY[0, 1, -1, 3, 4]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r3', ARRAY[1, -2]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r4', ARRAY[0, 1, -1]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r6', NULL);

--@test: array-set-functions - array_concat with literals
CREATE STREAM INPUT (id STRING KEY, dummy INTEGER) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_concat(array['foo', 'bar', 'foo'], array['foo', 'baz']) as a1 FROM INPUT;
INSERT INTO `INPUT` (ID, dummy) VALUES ('r1', 0);
INSERT INTO `INPUT` (ID, dummy) VALUES ('r2', 0);
ASSERT VALUES `OUTPUT` (ID, A1) VALUES ('r1', ARRAY['foo', 'bar', 'foo', 'foo', 'baz']);
ASSERT VALUES `OUTPUT` (ID, A1) VALUES ('r2', ARRAY['foo', 'bar', 'foo', 'foo', 'baz']);

--@test: array-set-functions - array_concat with primitive type
CREATE STREAM INPUT (id STRING KEY, arr1 ARRAY<INT>, arr2 ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_concat(arr1, arr2) as result FROM INPUT;
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r1', ARRAY[0, 0, 1, 0, -1], ARRAY[1, -2, 0]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r2', ARRAY[0, 0, 1, 0, -1], ARRAY[3, 4]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r3', ARRAY[], ARRAY[1, -2]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r4', ARRAY[0, 0, 1, 0, -1], ARRAY[]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r5', NULL, ARRAY[1, -2]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r6', ARRAY[0, 0, 1, 0, -1], NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', ARRAY[0, 0, 1, 0, -1, 1, -2, 0]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r2', ARRAY[0, 0, 1, 0, -1, 3, 4]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r3', ARRAY[1, -2]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r4', ARRAY[0, 0, 1, 0, -1]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', ARRAY[1, -2]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r6', ARRAY[0, 0, 1, 0, -1]);

--@test: array-set-functions - array_concat with left null
CREATE STREAM INPUT (id STRING KEY, arr1 ARRAY<INT>, arr2 ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_concat(arr1, arr2) as result FROM INPUT;
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r1', ARRAY[0, 0, 1, 0, -1], ARRAY[1, -2, 0]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r5', NULL, ARRAY[1, -2]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', ARRAY[0, 0, 1, 0, -1, 1, -2, 0]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', ARRAY[1, -2]);

--@test: array-set-functions - array_concat with right null
CREATE STREAM INPUT (id STRING KEY, arr1 ARRAY<INT>, arr2 ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_concat(arr1, arr2) as result FROM INPUT;
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r1', ARRAY[0, 0, 1, 0, -1], ARRAY[1, -2, 0]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r5', ARRAY[0, 3, 2], NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', ARRAY[0, 0, 1, 0, -1, 1, -2, 0]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', ARRAY[0, 3, 2]);

--@test: array-set-functions - array_concat with both null
CREATE STREAM INPUT (id STRING KEY, arr1 ARRAY<INT>, arr2 ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_concat(arr1, arr2) as result FROM INPUT;
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r1', ARRAY[0, 0, 1, 0, -1], ARRAY[1, -2, 0]);
INSERT INTO `INPUT` (ID, arr1, arr2) VALUES ('r5', NULL, NULL);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r1', ARRAY[0, 0, 1, 0, -1, 1, -2, 0]);
ASSERT VALUES `OUTPUT` (ID, RESULT) VALUES ('r5', NULL);

