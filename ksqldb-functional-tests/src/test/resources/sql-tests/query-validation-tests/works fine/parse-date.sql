--@test: parse-date - string to date
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, date varchar, format varchar) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select K, id, parse_date(date, format) as ts from test;
INSERT INTO `TEST` (K, ID, NAME, DATE, FORMAT) VALUES ('0', 0, 'zero', '2018-05-11Lit', 'yyyy-MM-dd''Lit''');
INSERT INTO `TEST` (K, ID, NAME, DATE, FORMAT) VALUES ('1', 1, 'zero', '11/05/2019', 'dd/MM/yyyy');
INSERT INTO `TEST` (K, ID, NAME, DATE, FORMAT) VALUES ('2', 2, 'zero', '01-Jan-2022', 'dd-MMM-yyyy');
INSERT INTO `TEST` (K, ID, NAME, DATE, FORMAT) VALUES ('3', 3, 'yyy', '01-01-1970', 'dd-MM-yyyy');
INSERT INTO `TEST` (K, ID, NAME, DATE, FORMAT) VALUES ('4', 4, 'yyy', '01-JAN-2022', 'dd-MMM-yyyy');
INSERT INTO `TEST` (K, ID, NAME, DATE, FORMAT) VALUES ('4', 4, 'yyy', 'JAN-2022', 'MMM-yyyy');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('0', 0, '2018-05-11');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('1', 1, '2019-05-11');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('2', 2, '2022-01-01');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('3', 3, '1970-01-01');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('4', 4, '2022-01-01');
ASSERT VALUES `TS` (K, ID, TS) VALUES ('4', 4, '2022-01-01');

