-- Tests around session windows

----------------------------------------------------------------------------------------------------
--@test: out of order - no grace period
----------------------------------------------------------------------------------------------------
CREATE STREAM TEST (ID BIGINT KEY, VALUE bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE S2 as SELECT ID, max(value) as max, windowstart as ws, windowend as we FROM test WINDOW SESSION (30 SECONDS) group by id;

INSERT INTO TEST (id, value, rowtime) VALUES (0, 0, 0);
INSERT INTO TEST (id, value, rowtime) VALUES (0, 1, 70010);
INSERT INTO TEST (id, value, rowtime) VALUES (0, 5, 10009);
INSERT INTO TEST (id, value, rowtime) VALUES (0, 6, 10010);
INSERT INTO TEST (id, value, rowtime) VALUES (1, 100, 10009);
INSERT INTO TEST (id, value, rowtime) VALUES (1, 101, 10010);
INSERT INTO TEST (id, value, rowtime) VALUES (1, 200, 86412022);
INSERT INTO TEST (id, value, rowtime) VALUES (1, 200, 60000);

ASSERT VALUES S2 (id, max, ws, we) VALUES (0, 0, 0, 0);
ASSERT VALUES S2 (id, max, ws, we) VALUES (0, 1, 70010, 70010);
ASSERT NULL VALUES S2 (id) KEY (0);
ASSERT VALUES S2 (id, max, ws, we) VALUES (0, 5, 0, 10009);
ASSERT NULL VALUES S2 (id) KEY (0);
ASSERT VALUES S2 (id, max, ws, we) VALUES (0, 6, 0, 10010);
ASSERT VALUES S2 (id, max, ws, we) VALUES (1, 100, 10009, 10009);
ASSERT NULL VALUES S2 (id) KEY (1);
ASSERT VALUES S2 (id, max, ws, we) VALUES (1, 101, 10009, 10010);
ASSERT VALUES S2 (id, max, ws, we) VALUES (1, 200, 86412022, 86412022);
ASSERT VALUES S2 (id, max, ws, we) VALUES (1, 200, 60000, 60000);

----------------------------------------------------------------------------------------------------
--@test: out of order - explicit grace period
----------------------------------------------------------------------------------------------------
CREATE STREAM TEST (ID BIGINT KEY, VALUE bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE S2 as SELECT ID, max(value) as max, windowstart as ws, windowend as we FROM test WINDOW SESSION (30 SECONDS, GRACE PERIOD 1 MINUTE) group by id;

INSERT INTO TEST (id, value, rowtime) VALUES (0, 0, 0);
INSERT INTO TEST (id, value, rowtime) VALUES (0, 1, 100010);
INSERT INTO TEST (id, value, rowtime) VALUES (0, 5, 10009);
INSERT INTO TEST (id, value, rowtime) VALUES (0, 6, 10010);
INSERT INTO TEST (id, value, rowtime) VALUES (1, 100, 10009);
INSERT INTO TEST (id, value, rowtime) VALUES (1, 101, 10010);
INSERT INTO TEST (id, value, rowtime) VALUES (1, 200, 86412022);
INSERT INTO TEST (id, value, rowtime) VALUES (1, 200, 60000);

ASSERT VALUES S2 (id, max, ws, we) VALUES (0, 0, 0, 0);
ASSERT VALUES S2 (id, max, ws, we) VALUES (0, 1, 100010, 100010);
ASSERT VALUES S2 (id, max, ws, we) VALUES (0, 6, 10010, 10010);
ASSERT VALUES S2 (id, max, ws, we) VALUES (1, 101, 10010, 10010);
ASSERT VALUES S2 (id, max, ws, we) VALUES (1, 200, 86412022, 86412022);

----------------------------------------------------------------------------------------------------
--@test: non-KAFKA key format
----------------------------------------------------------------------------------------------------
CREATE STREAM INPUT (A DECIMAL(4,2)) WITH (kafka_topic='INPUT', format='JSON');
CREATE TABLE OUTPUT AS SELECT A, COUNT() AS COUNT, windowstart as ws, windowend as we FROM INPUT WINDOW SESSION (30 SECONDS) group by A;

INSERT INTO INPUT (A, rowtime) VALUES (12.3, 10);
INSERT INTO INPUT (A, rowtime) VALUES (12.3, 11);
INSERT INTO INPUT (A, rowtime) VALUES (1, 12);

ASSERT VALUES OUTPUT (A, count, ws, we) VALUES (12.3, 1, 10, 10);
ASSERT NULL VALUES OUTPUT (A) KEY (12.3);
ASSERT VALUES OUTPUT (A, count, ws, we) VALUES (12.3, 2, 10, 11);
ASSERT VALUES OUTPUT (A, count, ws, we) VALUES (1, 1, 12, 12);
