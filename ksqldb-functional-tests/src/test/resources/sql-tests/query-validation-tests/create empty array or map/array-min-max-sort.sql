--@test: array-min-max-sort - array_max
CREATE STREAM INPUT (ID STRING KEY, bool_array ARRAY<BOOLEAN>, int_array ARRAY<INT>, bigint_array ARRAY<BIGINT>, double_array ARRAY<DOUBLE>, string_array ARRAY<STRING>, decimal_array ARRAY<DECIMAL(2,1)>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, array_max(bool_array) as bool_max, array_max(int_array) as int_max, array_max(bigint_array) as bigint_max, array_max(double_array) as double_max, array_max(string_array) as string_max, array_max(decimal_array) as decimal_max FROM INPUT;
INSERT INTO `INPUT` (ID, bool_array, int_array, bigint_array, double_array, string_array, decimal_array) VALUES ('r1', ARRAY[false, true, false], ARRAY[0, 0, 1, 0, -1], ARRAY[234, -123, 345], ARRAY[0.0, 0.1, -12345.678, 0.2, 0.3], ARRAY['foo', 'bar'], ARRAY[1.0, 1.1, 1.2, -0.2, 1.9, 9.0, -9.9, 1.5]);
INSERT INTO `INPUT` (ID, bool_array, int_array, bigint_array, double_array, string_array, decimal_array) VALUES ('r2', ARRAY[NULL, false, true], ARRAY[0, NULL, 1, 0, -1], ARRAY[NULL, -123, 345], ARRAY[0.0, 0.1, -12345.678, NULL, 0.3], ARRAY['foo', 'fo', 'Food', NULL, 'F', 'food'], ARRAY[1.0, 1.1, 1.2, -0.2, NULL, 9.0]);
INSERT INTO `INPUT` (ID, bool_array, int_array, bigint_array, double_array, string_array, decimal_array) VALUES ('r3', ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, BOOL_MAX, INT_MAX, BIGINT_MAX, DOUBLE_MAX, STRING_MAX, DECIMAL_MAX) VALUES ('r1', true, 1, 345, 0.3, 'foo', 9.0);
ASSERT VALUES `OUTPUT` (ID, BOOL_MAX, INT_MAX, BIGINT_MAX, DOUBLE_MAX, STRING_MAX, DECIMAL_MAX) VALUES ('r2', true, 1, 345, 0.3, 'food', 9.0);
ASSERT VALUES `OUTPUT` (ID, BOOL_MAX, INT_MAX, BIGINT_MAX, DOUBLE_MAX, STRING_MAX, DECIMAL_MAX) VALUES ('r3', NULL, NULL, NULL, NULL, NULL, NULL);

--@test: array-min-max-sort - array_min
CREATE STREAM INPUT (ID STRING KEY, bool_array ARRAY<BOOLEAN>, int_array ARRAY<INT>, bigint_array ARRAY<BIGINT>, double_array ARRAY<DOUBLE>, string_array ARRAY<STRING>, decimal_array ARRAY<DECIMAL(2,1)>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, array_min(bool_array) as bool_min, array_min(int_array) as int_min, array_min(bigint_array) as bigint_min, array_min(double_array) as double_min, array_min(string_array) as string_min, array_min(decimal_array) as decimal_min FROM INPUT;
INSERT INTO `INPUT` (ID, bool_array, int_array, bigint_array, double_array, string_array, decimal_array) VALUES ('r1', ARRAY[false, true, false], ARRAY[0, 0, 1, 0, -1], ARRAY[234, -123, 345], ARRAY[0.0, 0.1, -12345.678, 0.2, 0.3], ARRAY['foo', 'bar'], ARRAY[1.0, 1.1, 1.2, -0.2, 1.9, 9.0, -9.9, 1.5]);
INSERT INTO `INPUT` (ID, bool_array, int_array, bigint_array, double_array, string_array, decimal_array) VALUES ('r2', ARRAY[NULL, false, true], ARRAY[0, NULL, 1, 0, -1], ARRAY[NULL, -123, 345], ARRAY[0.0, 0.1, -12345.678, NULL, 0.3], ARRAY['foo', 'fo', 'Food', NULL, 'F', 'food'], ARRAY[1.0, 1.1, 1.2, -0.2, NULL, 9.0]);
INSERT INTO `INPUT` (ID, bool_array, int_array, bigint_array, double_array, string_array, decimal_array) VALUES ('r3', ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, BOOL_MIN, INT_MIN, BIGINT_MIN, DOUBLE_MIN, STRING_MIN, DECIMAL_MIN) VALUES ('r1', false, -1, -123, -12345.678, 'bar', -9.9);
ASSERT VALUES `OUTPUT` (ID, BOOL_MIN, INT_MIN, BIGINT_MIN, DOUBLE_MIN, STRING_MIN, DECIMAL_MIN) VALUES ('r2', false, -1, -123, -12345.678, 'F', -0.2);
ASSERT VALUES `OUTPUT` (ID, BOOL_MIN, INT_MIN, BIGINT_MIN, DOUBLE_MIN, STRING_MIN, DECIMAL_MIN) VALUES ('r3', NULL, NULL, NULL, NULL, NULL, NULL);

--@test: array-min-max-sort - array_sort_asc
CREATE STREAM INPUT (ID STRING KEY, bool_array ARRAY<BOOLEAN>, int_array ARRAY<INT>, bigint_array ARRAY<BIGINT>, double_array ARRAY<DOUBLE>, string_array ARRAY<STRING>, decimal_array ARRAY<DECIMAL(2,1)>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, array_sort(bool_array) as bools, array_sort(int_array) as ints, array_sort(bigint_array) as bigints, array_sort(double_array) as doubles, array_sort(string_array) as strings, array_sort(decimal_array) as decimals FROM INPUT;
INSERT INTO `INPUT` (ID, bool_array, int_array, bigint_array, double_array, string_array, decimal_array) VALUES ('r1', ARRAY[false, true, false], ARRAY[0, 0, 1, 0, -1], ARRAY[234, -123, 345], ARRAY[0.0, 0.1, -12345.678, 0.2, 0.3], ARRAY['foo', 'bar'], ARRAY[1.0, 1.1, -0.2, 1.9, 9.0, -9.9]);
INSERT INTO `INPUT` (ID, bool_array, int_array, bigint_array, double_array, string_array, decimal_array) VALUES ('r2', ARRAY[NULL, false, true], ARRAY[0, NULL, 1, 0, -1], ARRAY[NULL, -123, 345], ARRAY[0.0, 0.1, -12345.678, NULL, 0.3], ARRAY['foo', 'fo', 'Food', NULL, 'F', 'food'], ARRAY[1.0, 1.1, 1.2, -0.2, NULL, 9.0]);
INSERT INTO `INPUT` (ID, bool_array, int_array, bigint_array, double_array, string_array, decimal_array) VALUES ('r3', ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[]);
INSERT INTO `INPUT` (ID, bool_array, int_array, bigint_array, double_array, string_array, decimal_array) VALUES ('r4', NULL, NULL, NULL, NULL, NULL, NULL);
ASSERT VALUES `OUTPUT` (ID, BOOLS, INTS, BIGINTS, DOUBLES, STRINGS, DECIMALS) VALUES ('r1', ARRAY[false, false, true], ARRAY[-1, 0, 0, 0, 1], ARRAY[-123, 234, 345], ARRAY[-12345.678, 0.0, 0.1, 0.2, 0.3], ARRAY['bar', 'foo'], ARRAY[-9.9, -0.2, 1.0, 1.1, 1.9, 9.0]);
ASSERT VALUES `OUTPUT` (ID, BOOLS, INTS, BIGINTS, DOUBLES, STRINGS, DECIMALS) VALUES ('r2', ARRAY[false, true, NULL], ARRAY[-1, 0, 0, 1, NULL], ARRAY[-123, 345, NULL], ARRAY[-12345.678, 0.0, 0.1, 0.3, NULL], ARRAY['F', 'Food', 'fo', 'foo', 'food', NULL], ARRAY[-0.2, 1.0, 1.1, 1.2, 9.0, NULL]);
ASSERT VALUES `OUTPUT` (ID, BOOLS, INTS, BIGINTS, DOUBLES, STRINGS, DECIMALS) VALUES ('r3', ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, BOOLS, INTS, BIGINTS, DOUBLES, STRINGS, DECIMALS) VALUES ('r4', NULL, NULL, NULL, NULL, NULL, NULL);

--@test: array-min-max-sort - array_sort_desc
CREATE STREAM INPUT (ID STRING KEY, bool_array ARRAY<BOOLEAN>, int_array ARRAY<INT>, bigint_array ARRAY<BIGINT>, double_array ARRAY<DOUBLE>, string_array ARRAY<STRING>, decimal_array ARRAY<DECIMAL(2,1)>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, array_sort(bool_array, 'desc') as bools, array_sort(int_array, 'desc') as ints, array_sort(bigint_array, 'desc') as bigints, array_sort(double_array, 'desc') as doubles, array_sort(string_array, 'desc') as strings, array_sort(decimal_array, 'desc') as decimals FROM INPUT;
INSERT INTO `INPUT` (ID, bool_array, int_array, bigint_array, double_array, string_array, decimal_array) VALUES ('r1', ARRAY[false, true, false], ARRAY[0, 0, 1, 0, -1], ARRAY[234, -123, 345], ARRAY[0.0, 0.1, -12345.678, 0.2, 0.3], ARRAY['foo', 'bar'], ARRAY[1.0, 1.1, -0.2, 1.9, 9.0, -9.9]);
INSERT INTO `INPUT` (ID, bool_array, int_array, bigint_array, double_array, string_array, decimal_array) VALUES ('r2', ARRAY[NULL, false, true], ARRAY[0, NULL, 1, 0, -1], ARRAY[NULL, -123, 345], ARRAY[0.0, 0.1, -12345.678, NULL, 0.3], ARRAY['foo', 'fo', 'Food', NULL, 'F', 'food'], ARRAY[1.0, 1.1, 1.2, -0.2, NULL, 9.0]);
INSERT INTO `INPUT` (ID, bool_array, int_array, bigint_array, double_array, string_array, decimal_array) VALUES ('r3', ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[]);
INSERT INTO `INPUT` (ID, bool_array, int_array, bigint_array, double_array, string_array, decimal_array) VALUES ('r4', NULL, NULL, NULL, NULL, NULL, NULL);
ASSERT VALUES `OUTPUT` (ID, BOOLS, INTS, BIGINTS, DOUBLES, STRINGS, DECIMALS) VALUES ('r1', ARRAY[true, false, false], ARRAY[1, 0, 0, 0, -1], ARRAY[345, 234, -123], ARRAY[0.3, 0.2, 0.1, 0.0, -12345.678], ARRAY['foo', 'bar'], ARRAY[9.0, 1.9, 1.1, 1.0, -0.2, -9.9]);
ASSERT VALUES `OUTPUT` (ID, BOOLS, INTS, BIGINTS, DOUBLES, STRINGS, DECIMALS) VALUES ('r2', ARRAY[true, false, NULL], ARRAY[1, 0, 0, -1, NULL], ARRAY[345, -123, NULL], ARRAY[0.3, 0.1, 0.0, -12345.678, NULL], ARRAY['food', 'foo', 'fo', 'Food', 'F', NULL], ARRAY[9.0, 1.2, 1.1, 1.0, -0.2, NULL]);
ASSERT VALUES `OUTPUT` (ID, BOOLS, INTS, BIGINTS, DOUBLES, STRINGS, DECIMALS) VALUES ('r3', ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[]);
ASSERT VALUES `OUTPUT` (ID, BOOLS, INTS, BIGINTS, DOUBLES, STRINGS, DECIMALS) VALUES ('r4', NULL, NULL, NULL, NULL, NULL, NULL);

