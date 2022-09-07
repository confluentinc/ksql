--@test: array-remove - array_remove with literal array
CREATE STREAM INPUT (id STRING KEY, dummy INTEGER) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_remove(array['foo', 'bar', 'foo'], 'foo') as a1 FROM INPUT;
INSERT INTO `INPUT` (ID, dummy) VALUES ('r1', 0);
INSERT INTO `INPUT` (ID, dummy) VALUES ('r2', 0);
ASSERT VALUES `OUTPUT` (ID, A1) VALUES ('r1', ARRAY['bar']);
ASSERT VALUES `OUTPUT` (ID, A1) VALUES ('r2', ARRAY['bar']);

--@test: array-remove - array_remove with all primitive types
CREATE STREAM INPUT (id STRING KEY, bools ARRAY<BOOLEAN>, bad_bool BOOLEAN, ints ARRAY<INT>, bad_int INT, bigints ARRAY<BIGINT>, bad_bigint BIGINT, doubles ARRAY<DOUBLE>, bad_double DOUBLE, strings ARRAY<STRING>, bad_string STRING, decimals ARRAY<DECIMAL(2,1)>, bad_decimal DECIMAL(2,1)) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_remove(bools, bad_bool) as bools, array_remove(ints, bad_int) as ints, array_remove(bigints, bad_bigint) as bigints, array_remove(doubles, bad_double) as doubles, array_remove(strings, bad_string) as strings, array_remove(decimals, bad_decimal) as decimals FROM INPUT;
INSERT INTO `INPUT` (ID, bools, bad_bool, ints, bad_int, bigints, bad_bigint, doubles, bad_double, strings, bad_string, decimals, bad_decimal) VALUES ('r1', ARRAY[false, true, false], true, ARRAY[0, 0, 1, 0, -1], -1, ARRAY[345, -123, 345], 345, ARRAY[0.0, 0.2, -12345.678, 0.2], 0.2, ARRAY['foo', 'bar', 'foo'], 'foo', ARRAY[1.0, -0.2, 1.0, -9.9], -0.2);
INSERT INTO `INPUT` (ID, bools, bad_bool, ints, bad_int, bigints, bad_bigint, doubles, bad_double, strings, bad_string, decimals, bad_decimal) VALUES ('r2', ARRAY[NULL, false, true], true, ARRAY[0, NULL, 1, 0, -1], -1, ARRAY[NULL, -123, 345], 345, ARRAY[0.3, -12345.678, NULL, 0.3], 0.3, ARRAY['foo', 'Food', NULL, 'food'], 'foo', ARRAY[1.0, 1.1, 1.1, -0.2, NULL, 1.0], -0.2);
INSERT INTO `INPUT` (ID, bools, bad_bool, ints, bad_int, bigints, bad_bigint, doubles, bad_double, strings, bad_string, decimals, bad_decimal) VALUES ('r3', ARRAY[NULL, false, true], NULL, ARRAY[0, NULL, 1, 0, -1], NULL, ARRAY[NULL, -123, 345], NULL, ARRAY[0.3, -12345.678, NULL, 0.3], NULL, ARRAY['foo', 'Food', NULL, 'food'], NULL, ARRAY[1.0, 1.1, 1.1, -0.2, NULL, 1.0], NULL);
INSERT INTO `INPUT` (ID, bools, bad_bool, ints, bad_int, bigints, bad_bigint, doubles, bad_double, strings, bad_string, decimals, bad_decimal) VALUES ('r4', ARRAY[], true, ARRAY[], -1, ARRAY[], 345, ARRAY[], 0.2, ARRAY[], 'foo', ARRAY[], -0.2);
INSERT INTO `INPUT` (ID, bools, bad_bool, ints, bad_int, bigints, bad_bigint, doubles, bad_double, strings, bad_string, decimals, bad_decimal) VALUES ('r5', NULL, true, NULL, -1, NULL, 345, NULL, 0.2, NULL, 'foo', NULL, -0.2);
ASSERT VALUES `OUTPUT` (ID, BOOLS, INTS, BIGINTS, DOUBLES, STRINGS, DECIMALS) VALUES ('r1', ARRAY[false, false], ARRAY[0, 0, 1, 0], ARRAY[-123], ARRAY[0.0, -12345.678], ARRAY['bar'], ARRAY[1.0, 1.0, -9.9]);
ASSERT VALUES `OUTPUT` (ID, BOOLS, INTS, BIGINTS, DOUBLES, STRINGS, DECIMALS) VALUES ('r2', ARRAY[NULL, false], ARRAY[0, NULL, 1, 0], ARRAY[NULL, -123], ARRAY[-12345.678, NULL], ARRAY['Food', NULL, 'food'], ARRAY[1.0, 1.1, 1.1, NULL, 1.0]);
ASSERT VALUES `OUTPUT` (ID, BOOLS, INTS, BIGINTS, DOUBLES, STRINGS, DECIMALS) VALUES ('r3', ARRAY[false, true], ARRAY[0, 1, 0, -1], ARRAY[-123, 345], ARRAY[0.3, -12345.678, 0.3], ARRAY['foo', 'Food', 'food'], ARRAY[1.0, 1.1, 1.1, -0.2, 1.0]);
ASSERT VALUES `OUTPUT` (ID, BOOLS, INTS, BIGINTS, DOUBLES, STRINGS, DECIMALS) VALUES ('r4', ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, BOOLS, INTS, BIGINTS, DOUBLES, STRINGS, DECIMALS) VALUES ('r5', NULL, NULL, NULL, NULL, NULL, NULL);

--@test: array-remove - array_remove with complex types
CREATE STREAM INPUT (id STRING KEY, lists ARRAY<ARRAY<STRING>>, bad_list ARRAY<STRING>, maps ARRAY<MAP<STRING,INT>>, bad_map MAP<STRING, INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT id, array_remove(lists, bad_list) as lists, array_remove(maps, bad_map) as maps FROM INPUT;
INSERT INTO `INPUT` (ID, lists, bad_list, maps, bad_map) VALUES ('r1', ARRAY[ARRAY['foo', 'bar', 'foo'], ARRAY['foo', 'bar', 'foo'], ARRAY['foo']], ARRAY['foo', 'bar', 'foo'], ARRAY[MAP('apple':=1, 'banana':=2), MAP('apple':=3, 'banana':=4), MAP('apple':=1, 'banana':=2)], MAP('apple':=1, 'banana':=2));
INSERT INTO `INPUT` (ID, lists, bad_list, maps, bad_map) VALUES ('r2', ARRAY[NULL, ARRAY['foo'], ARRAY['foo', 'bar']], ARRAY['foo'], ARRAY[MAP('apple':=1, 'banana':=2), MAP('apple':=3, 'banana':=4), NULL], MAP('apple':=1, 'banana':=2));
INSERT INTO `INPUT` (ID, lists, bad_list, maps) VALUES ('r3', ARRAY[], ARRAY['foo'], ARRAY[]);
INSERT INTO `INPUT` (ID, lists, bad_list, maps, bad_map) VALUES ('r4', NULL, ARRAY['foo'], NULL, MAP('apple':=1, 'banana':=2));
ASSERT VALUES `OUTPUT` (ID, LISTS, MAPS) VALUES ('r1', ARRAY[ARRAY['foo']], ARRAY[MAP('apple':=3, 'banana':=4)]);
ASSERT VALUES `OUTPUT` (ID, LISTS, MAPS) VALUES ('r2', ARRAY[NULL, ARRAY['foo', 'bar']], ARRAY[MAP('apple':=3, 'banana':=4), NULL]);
ASSERT VALUES `OUTPUT` (ID, LISTS, MAPS) VALUES ('r3', ARRAY[], ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, LISTS, MAPS) VALUES ('r4', NULL, NULL);

