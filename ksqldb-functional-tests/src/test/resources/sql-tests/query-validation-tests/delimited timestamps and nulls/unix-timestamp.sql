--@test: unix-timestamp - returns the current timestamp
CREATE STREAM TEST (K STRING KEY, name STRING) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select K, unix_timestamp() > 100 as ts from test;
INSERT INTO `TEST` (K, NAME) VALUES ('0', 'a');
INSERT INTO `TEST` (K, NAME) VALUES ('0', 'b');
INSERT INTO `TEST` (K) VALUES ('0');
ASSERT VALUES `TS` (K, TS) VALUES ('0', true);
ASSERT VALUES `TS` (K, TS) VALUES ('0', true);
ASSERT VALUES `TS` (K) VALUES ('0');

--@test: unix-timestamp - convert timezone to milliseconds
CREATE STREAM TEST (K STRING KEY, time TIMESTAMP) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select K, unix_timestamp(time) as ts from test;
INSERT INTO `TEST` (K, TIME) VALUES ('0', '1970-01-01T00:00:00.100');
INSERT INTO `TEST` (K, TIME) VALUES ('0', '2020-05-11T21:58:33.000');
INSERT INTO `TEST` (K) VALUES ('0');
ASSERT VALUES `TS` (K, TS) VALUES ('0', 100);
ASSERT VALUES `TS` (K, TS) VALUES ('0', 1589234313000);
ASSERT VALUES `TS` (K) VALUES ('0');

