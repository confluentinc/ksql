--@test: average - calculate average in select
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE AVG AS select ID, abs(sum(value)/count(id)) * 10 as avg from test GROUP BY id;
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', -50);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', -10);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', -15);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (1, 'one', 100);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (1, 'one', 10);
ASSERT VALUES `AVG` (ID, AVG) VALUES (0, 500);
ASSERT VALUES `AVG` (ID, AVG) VALUES (0, 300);
ASSERT VALUES `AVG` (ID, AVG) VALUES (0, 250);
ASSERT VALUES `AVG` (ID, AVG) VALUES (1, 1000);
ASSERT VALUES `AVG` (ID, AVG) VALUES (1, 550);

