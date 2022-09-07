--@test: unix-date - returns the current date
CREATE STREAM TEST (K STRING KEY, name STRING) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select K, unix_date() > 100 as ts from test;
INSERT INTO `TEST` (K, NAME) VALUES ('0', 'a');
INSERT INTO `TEST` (K, NAME) VALUES ('0', 'b');
INSERT INTO `TEST` (K) VALUES ('0');
ASSERT VALUES `TS` (K, TS) VALUES ('0', true);
ASSERT VALUES `TS` (K, TS) VALUES ('0', true);
ASSERT VALUES `TS` (K) VALUES ('0');

--@test: unix-date - convert date to days
CREATE STREAM TEST (K STRING KEY, date DATE) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select K, unix_date(date) as ts from test;
INSERT INTO `TEST` (K, DATE) VALUES ('0', '1970-01-11');
INSERT INTO `TEST` (K, DATE) VALUES ('0', '1970-04-11');
INSERT INTO `TEST` (K) VALUES ('0');
ASSERT VALUES `TS` (K, TS) VALUES ('0', 10);
ASSERT VALUES `TS` (K, TS) VALUES ('0', 100);
ASSERT VALUES `TS` (K) VALUES ('0');

