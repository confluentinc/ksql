--@test: having - table having
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE T1 as select id, sum(value) as sum from test WINDOW TUMBLING (SIZE 30 SECONDS) group by id HAVING sum(value) > 100;
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 0);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (1, 'one', 101);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (1, 'one', -5);
ASSERT VALUES `T1` (ID, SUM, WINDOWSTART, WINDOWEND) VALUES (1, 101, 0, 30000);
ASSERT VALUES `T1` (ID, WINDOWSTART, WINDOWEND) VALUES (1, 0, 30000);

--@test: having - calculate average in having
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE AVG AS select id, sum(value)/count(id) as avg from test GROUP BY id HAVING sum(value)/count(id)> 25;
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 50);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 10);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (0, 'zero', 15);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (1, 'one', 100);
INSERT INTO `TEST` (ID, NAME, VALUE) VALUES (1, 'one', 10);
ASSERT VALUES `AVG` (ID, AVG) VALUES (0, 50);
ASSERT VALUES `AVG` (ID, AVG) VALUES (0, 30);
ASSERT NULL VALUES `AVG` (ID) KEY (0);
ASSERT VALUES `AVG` (ID, AVG) VALUES (1, 100);
ASSERT VALUES `AVG` (ID, AVG) VALUES (1, 55);

