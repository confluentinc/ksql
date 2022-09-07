--@test: udf-implicit-cast - int literal -> double
CREATE STREAM TEST (K STRING KEY, ID bigint, LAT1 double, LON1 double) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM DISTANCE_STREAM AS select K, ID, geo_distance(LAT1, LON1, 51, 0) as CALCULATED_DISTANCE from test;
INSERT INTO `TEST` (ID, LAT1, LON1) VALUES (1, 37.4439, -122.1663);
ASSERT VALUES `DISTANCE_STREAM` (ID, CALCULATED_DISTANCE) VALUES (1, 8682.459061368269);

--@test: udf-implicit-cast - int field -> double
CREATE STREAM TEST (K STRING KEY, ID bigint, LAT1 double, LON1 double, LAT2 int) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM DISTANCE_STREAM AS select K, ID, geo_distance(LAT1, LON1, LAT2, 0) as CALCULATED_DISTANCE from test;
INSERT INTO `TEST` (ID, LAT1, LON1, LAT2) VALUES (1, 37.4439, -122.1663, 51);
ASSERT VALUES `DISTANCE_STREAM` (ID, CALCULATED_DISTANCE) VALUES (1, 8682.459061368269);

--@test: udf-implicit-cast - long field -> double
CREATE STREAM TEST (K STRING KEY, ID bigint, LAT1 double, LON1 double, LAT2 bigint) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM DISTANCE_STREAM AS select K, ID, geo_distance(LAT1, LON1, LAT2, 0) as CALCULATED_DISTANCE from test;
INSERT INTO `TEST` (ID, LAT1, LON1, LAT2) VALUES (1, 37.4439, -122.1663, 51);
ASSERT VALUES `DISTANCE_STREAM` (ID, CALCULATED_DISTANCE) VALUES (1, 8682.459061368269);

--@test: udf-implicit-cast - decimal literal -> double
CREATE STREAM TEST (K STRING KEY, ID bigint, LAT1 double, LON1 double) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM DISTANCE_STREAM AS select K, ID, geo_distance(LAT1, LON1, 51.0, 0) as CALCULATED_DISTANCE from test;
INSERT INTO `TEST` (ID, LAT1, LON1) VALUES (1, 37.4439, -122.1663);
ASSERT VALUES `DISTANCE_STREAM` (ID, CALCULATED_DISTANCE) VALUES (1, 8682.459061368269);

--@test: udf-implicit-cast - decimal field -> double
CREATE STREAM TEST (K STRING KEY, ID bigint, LAT1 double, LON1 double, LAT2 decimal(3, 1)) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM DISTANCE_STREAM AS select K, ID, geo_distance(LAT1, LON1, LAT2, 0) as CALCULATED_DISTANCE from test;
INSERT INTO `TEST` (ID, LAT1, LON1, LAT2) VALUES (1, 37.4439, -122.1663, 51.0);
ASSERT VALUES `DISTANCE_STREAM` (ID, CALCULATED_DISTANCE) VALUES (1, 8682.459061368269);

--@test: udf-implicit-cast - choose the exact match first
CREATE STREAM TEST (K STRING KEY, ID int) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS select K, ID, test_udf(ID, 'foo') as foo from test;
INSERT INTO `TEST` (ID) VALUES (1);
ASSERT VALUES `OUTPUT` (ID, FOO) VALUES (1, 'doStuffIntString');

--@test: udf-implicit-cast - string literal -> double
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Function 'geo_distance' does not accept parameters
CREATE STREAM TEST (K STRING KEY, ID bigint, LAT1 double, LON1 double) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM DISTANCE_STREAM AS select K, ID, geo_distance(LAT1, LON1, 'foo', 0) as CALCULATED_DISTANCE from test;
