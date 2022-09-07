--@test: timestampsub - subtracts
CREATE STREAM TEST (ID STRING KEY, time TIMESTAMP) WITH (kafka_topic='test', value_format='DELIMITED');
CREATE STREAM TEST2 AS SELECT id, timestampsub(MILLISECONDS, 10, time) FROM TEST;
INSERT INTO `TEST` (TIME) VALUES ('1970-01-01T00:00:00.020');
ASSERT VALUES `TEST2` (KSQL_COL_0) VALUES ('1970-01-01T00:00:00.010');

--@test: timestampsub - throws on incorrect type
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Function 'timestampsub' does not accept parameters (TIMESTAMP, INTEGER, TIMESTAMP).
CREATE STREAM TEST (ID STRING KEY, time TIMESTAMP) WITH (kafka_topic='test', value_format='DELIMITED');
CREATE STREAM TEST2 AS SELECT id, timestampsub(time, 5, time) FROM TEST;
--@test: timestampsub - subtracts negative intervals
CREATE STREAM TEST (ID INT KEY, time TIMESTAMP) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT id, timestampsub(MILLISECONDS, -5, time) AS VALUE FROM TEST;
INSERT INTO `TEST` (ID, time) VALUES (0, '1970-01-01T00:00:00.005');
INSERT INTO `TEST` (ID, time) VALUES (0, '1970-01-01T00:00:00.000');
INSERT INTO `TEST` (ID, time) VALUES (0, '1969-12-31T23:59:59.900');
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, '1970-01-01T00:00:00.010');
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, '1970-01-01T00:00:00.005');
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, '1969-12-31T23:59:59.905');

--@test: timestampsub - handles null values
CREATE STREAM TEST (ID INT KEY, time TIMESTAMP, num INTEGER) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT id, timestampsub(MILLISECONDS, num, time) AS VALUE FROM TEST;
INSERT INTO `TEST` (ID, time, num) VALUES (0, NULL, 5);
INSERT INTO `TEST` (ID, time, num) VALUES (0, '1970-01-01T00:00:00.005', NULL);
INSERT INTO `TEST` (ID, time, num) VALUES (0, NULL, NULL);
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, NULL);
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, NULL);
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, NULL);

