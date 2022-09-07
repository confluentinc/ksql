--@test: from-days - convert days to date
CREATE STREAM TEST (K STRING KEY, d INT) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select K, from_days(d) as ts from test;
INSERT INTO `TEST` (K, D) VALUES ('0', 10);
INSERT INTO `TEST` (K, D) VALUES ('0', -10);
INSERT INTO `TEST` (K) VALUES ('0');
ASSERT VALUES `TS` (K, TS) VALUES ('0', '1970-01-11');
ASSERT VALUES `TS` (K, TS) VALUES ('0', '1969-12-22');
ASSERT VALUES `TS` (K) VALUES ('0');

