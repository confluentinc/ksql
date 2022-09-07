--@test: stringtimestamp - string to timestamp
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, timestamp varchar) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select K, id, stringtotimestamp(timestamp, 'yyyy-MM-dd''T''HH:mm:ssX') as ts from test;
INSERT INTO `TEST` (K, ID, NAME, TIMESTAMP) VALUES ('0', 0, 'zero', '2018-05-11T21:58:33Z');
INSERT INTO `TEST` (K, ID, NAME, TIMESTAMP) VALUES ('0', 0, 'zero', '2019-05-11T21:58:33Z');
INSERT INTO `TEST` (K, ID, NAME, TIMESTAMP) VALUES ('0', 0, 'zero', '2020-05-11T21:58:33Z');
INSERT INTO `TEST` (K, ID, NAME, TIMESTAMP) VALUES ('0', 0, 'zero', '2020-05-11T21:58:33z');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('0', 0, 1526075913000);
ASSERT VALUES `TS` (K, ID, TS) VALUES ('0', 0, 1557611913000);
ASSERT VALUES `TS` (K, ID, TS) VALUES ('0', 0, 1589234313000);
ASSERT VALUES `TS` (K, ID, TS) VALUES ('0', 0, 1589234313000);

--@test: stringtimestamp - format timestamp
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, time varchar) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select K, id, parse_timestamp(time, 'yyyy-MM-dd''T''HH:mm:ss') as ts from test;
INSERT INTO `TEST` (K, ID, NAME, TIME) VALUES ('0', 0, 'zero', '2018-05-11T21:58:33');
INSERT INTO `TEST` (K, ID, NAME, TIME) VALUES ('0', 0, 'zero', '2019-05-11T21:58:33');
INSERT INTO `TEST` (K, ID, NAME, TIME) VALUES ('0', 0, 'zero', '2020-05-11T21:58:33');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('0', 0, '2018-05-11T21:58:33.000');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('0', 0, '2019-05-11T21:58:33.000');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('0', 0, '2020-05-11T21:58:33.000');

--@test: stringtimestamp - format timestamp with time zone
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, time varchar) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select K, id, parse_timestamp(time, 'yyyy-MM-dd''T''HH:mm:ss zzz') as ts from test;
INSERT INTO `TEST` (K, ID, NAME, TIME) VALUES ('0', 0, 'zero', '2018-05-11T21:58:33 PST');
INSERT INTO `TEST` (K, ID, NAME, TIME) VALUES ('0', 0, 'zero', '2019-05-11T21:58:33 PST');
INSERT INTO `TEST` (K, ID, NAME, TIME) VALUES ('0', 0, 'zero', '2020-05-11T21:58:33 PST');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('0', 0, '2018-05-12T04:58:33.000');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('0', 0, '2019-05-12T04:58:33.000');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('0', 0, '2020-05-12T04:58:33.000');

