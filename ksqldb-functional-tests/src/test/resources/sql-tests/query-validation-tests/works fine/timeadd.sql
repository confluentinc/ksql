--@test: timeadd - adds
CREATE STREAM TEST (ID STRING KEY, time TIME) WITH (kafka_topic='test', value_format='DELIMITED');
CREATE STREAM TEST2 AS SELECT id, timeadd(MILLISECONDS, 10, time) FROM TEST;
INSERT INTO `TEST` (TIME) VALUES ('00:00');
INSERT INTO `TEST` (TIME) VALUES ('23:59:59.999');
ASSERT VALUES `TEST2` (KSQL_COL_0) VALUES ('00:00:00.010');
ASSERT VALUES `TEST2` (KSQL_COL_0) VALUES ('00:00:00.009');

--@test: timeadd - throws on incorrect type
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Function 'timeadd' does not accept parameters (TIME, INTEGER, TIME).
CREATE STREAM TEST (ID STRING KEY, time TIME) WITH (kafka_topic='test', value_format='DELIMITED');
CREATE STREAM TEST2 AS SELECT id, timeadd(time, 5, time) FROM TEST;
--@test: timeadd - adds negative intervals
CREATE STREAM TEST (ID INT KEY, time TIME) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT id, timeadd(MILLISECONDS, -5, time) AS VALUE FROM TEST;
INSERT INTO `TEST` (ID, time) VALUES (0, '00:00:01');
INSERT INTO `TEST` (ID, time) VALUES (0, '00:00:00.005');
INSERT INTO `TEST` (ID, time) VALUES (0, '00:00');
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, '00:00:00.995');
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, '00:00');
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, '23:59:59.995');

--@test: timeadd - handles null values
CREATE STREAM TEST (ID INT KEY, time TIME, num INTEGER) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT id, timeadd(MILLISECONDS, num, time) AS VALUE FROM TEST;
INSERT INTO `TEST` (ID, time, num) VALUES (0, NULL, 5);
INSERT INTO `TEST` (ID, time, num) VALUES (0, '00:00', NULL);
INSERT INTO `TEST` (ID, time, num) VALUES (0, NULL, NULL);
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, NULL);
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, NULL);
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, NULL);

