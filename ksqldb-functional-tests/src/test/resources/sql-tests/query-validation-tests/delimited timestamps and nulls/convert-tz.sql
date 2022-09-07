--@test: convert-tz - convert timezones defined by offset
CREATE STREAM TEST (K STRING KEY, time TIMESTAMP) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select K, convert_tz(time, '+0200', '+0500') as ts from test;
INSERT INTO `TEST` (K, TIME) VALUES ('0', '1970-01-01T00:00:00.100');
INSERT INTO `TEST` (K, TIME) VALUES ('0', '2020-05-11T21:58:33.000');
INSERT INTO `TEST` (K) VALUES ('0');
ASSERT VALUES `TS` (K, TS) VALUES ('0', '1970-01-01T03:00:00.100');
ASSERT VALUES `TS` (K, TS) VALUES ('0', '2020-05-12T00:58:33.000');
ASSERT VALUES `TS` (K) VALUES ('0');

--@test: convert-tz - convert timezones defined by name
CREATE STREAM TEST (K STRING KEY, time TIMESTAMP) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select K, convert_tz(time, 'America/Los_Angeles', 'America/New_York') as ts from test;
INSERT INTO `TEST` (K, TIME) VALUES ('0', '1970-01-01T00:00:00.100');
INSERT INTO `TEST` (K, TIME) VALUES ('0', '2020-05-11T21:58:33.000');
INSERT INTO `TEST` (K) VALUES ('0');
ASSERT VALUES `TS` (K, TS) VALUES ('0', '1970-01-01T03:00:00.100');
ASSERT VALUES `TS` (K, TS) VALUES ('0', '2020-05-12T00:58:33.000');
ASSERT VALUES `TS` (K) VALUES ('0');

