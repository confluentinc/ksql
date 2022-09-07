--@test: dateadd - adds
CREATE STREAM TEST (ID STRING KEY, date DATE) WITH (kafka_topic='test', value_format='DELIMITED');
CREATE STREAM TEST2 AS SELECT id, dateadd(DAYS, 10, date) FROM TEST;
INSERT INTO `TEST` (DATE) VALUES ('1970-01-11');
ASSERT VALUES `TEST2` (KSQL_COL_0) VALUES ('1970-01-21');

--@test: dateadd - throws on incorrect type
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Function 'dateadd' does not accept parameters (DATE, INTEGER, DATE).
CREATE STREAM TEST (ID STRING KEY, date DATE) WITH (kafka_topic='test', value_format='DELIMITED');
CREATE STREAM TEST2 AS SELECT id, dateadd(date, 5, date) FROM TEST;
--@test: dateadd - adds negative intervals
CREATE STREAM TEST (ID INT KEY, date DATE) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT id, dateadd(DAYS, -5, date) AS VALUE FROM TEST;
INSERT INTO `TEST` (ID, date) VALUES (0, '1970-01-06');
INSERT INTO `TEST` (ID, date) VALUES (0, '1970-01-11');
INSERT INTO `TEST` (ID, date) VALUES (0, '1970-01-03');
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, '1970-01-01');
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, '1970-01-06');
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, '1969-12-29');

--@test: dateadd - handles null values
CREATE STREAM TEST (ID INT KEY, date DATE, num INTEGER) WITH (kafka_topic='test', value_format='AVRO');
CREATE STREAM TEST2 AS SELECT id, dateadd(MILLISECONDS, num, date) AS VALUE FROM TEST;
INSERT INTO `TEST` (ID, date, num) VALUES (0, NULL, 5);
INSERT INTO `TEST` (ID, date, num) VALUES (0, '1970-01-06', NULL);
INSERT INTO `TEST` (ID, date, num) VALUES (0, NULL, NULL);
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, NULL);
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, NULL);
ASSERT VALUES `TEST2` (ID, VALUE) VALUES (0, NULL);

