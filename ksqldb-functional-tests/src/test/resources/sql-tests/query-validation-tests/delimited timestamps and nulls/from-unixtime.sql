--@test: from-unixtime - convert milliseconds to timezone
CREATE STREAM TEST (K STRING KEY, millis BIGINT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select K, from_unixtime(millis) as ts from test;
INSERT INTO `TEST` (K, MILLIS) VALUES ('0', 100);
INSERT INTO `TEST` (K, MILLIS) VALUES ('0', 1589234313000);
INSERT INTO `TEST` (K) VALUES ('0');
ASSERT VALUES `TS` (K, TS) VALUES ('0', '1970-01-01T00:00:00.100');
ASSERT VALUES `TS` (K, TS) VALUES ('0', '2020-05-11T21:58:33.000');
ASSERT VALUES `TS` (K) VALUES ('0');

