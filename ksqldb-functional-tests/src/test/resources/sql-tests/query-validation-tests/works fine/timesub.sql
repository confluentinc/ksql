--@test: timesub - subtracts
CREATE STREAM TEST (ID STRING KEY, time TIME) WITH (kafka_topic='test', value_format='DELIMITED');
CREATE STREAM TEST2 AS SELECT id, timesub(MILLISECONDS, 10, time) FROM TEST;
INSERT INTO `TEST` (TIME) VALUES ('00:00:00.010');
INSERT INTO `TEST` (TIME) VALUES ('00:00:00.005');
ASSERT VALUES `TEST2` (KSQL_COL_0) VALUES ('00:00');
ASSERT VALUES `TEST2` (KSQL_COL_0) VALUES ('23:59:59.995');

--@test: timesub - throws on incorrect type
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Function 'timesub' does not accept parameters (TIME, INTEGER, TIME).
CREATE STREAM TEST (ID STRING KEY, time TIME) WITH (kafka_topic='test', value_format='DELIMITED');
CREATE STREAM TEST2 AS SELECT id, timesub(time, 5, time) FROM TEST;

--@test: timesub - subtracts negative intervals
CREATE STREAM TEST (ID INT KEY, time TIME) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT id, timesub(MILLISECONDS, -5, time) AS VALUE FROM TEST;
INSERT INTO `TEST` (ID, time) VALUES (0, '00:00');
INSERT INTO `TEST` (ID, time) VALUES (0, '00:00:00.995');
INSERT INTO `TEST` (ID, time) VALUES (0, '23:59:59.995');
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, '00:00:00.005');
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, '00:00:01');
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, '00:00');

--@test: timesub - handles null values
CREATE STREAM TEST (ID INT KEY, time TIME, num INTEGER) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT id, timesub(MILLISECONDS, num, time) AS VALUE FROM TEST;
INSERT INTO `TEST` (ID, time, num) VALUES (0, NULL, 5);
INSERT INTO `TEST` (ID, time, num) VALUES (0, '00:00', NULL);
INSERT INTO `TEST` (ID, time, num) VALUES (0, NULL, NULL);
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, NULL);
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, NULL);
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, NULL);

