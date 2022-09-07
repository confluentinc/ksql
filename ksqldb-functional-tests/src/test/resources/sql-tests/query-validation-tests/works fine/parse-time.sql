--@test: parse-time - string to time
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, time varchar, format varchar) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select K, id, parse_time(time, format) as ts from test;
INSERT INTO `TEST` (K, ID, NAME, TIME, FORMAT) VALUES ('0', 0, 'zero', '00:01:05Lit', 'HH:mm:ss''Lit''');
INSERT INTO `TEST` (K, ID, NAME, TIME, FORMAT) VALUES ('1', 1, 'zero', '11/05/19', 'HH/mm/ss');
INSERT INTO `TEST` (K, ID, NAME, TIME, FORMAT) VALUES ('2', 2, 'zero', '01:00:00 PM', 'hh:mm:ss a');
INSERT INTO `TEST` (K, ID, NAME, TIME, FORMAT) VALUES ('3', 3, 'zero', '01:00:00 Pm', 'hh:mm:ss a');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('0', 0, '00:01:05');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('1', 1, '11:05:19');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('2', 2, '13:00');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('3', 3, '13:00');

