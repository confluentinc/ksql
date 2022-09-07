--@test: multi-joins - first join column in projection
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT S1.ID, s1.V0, t2.V0, t3.V0 FROM S1 JOIN T2 ON S1.ID = T2.ID JOIN T3 ON S1.ID = T3.ID;
INSERT INTO `T3` (ID, V0, ROWTIME) VALUES (0, 3, 10);
INSERT INTO `T2` (ID, V0, ROWTIME) VALUES (0, 2, 11);
INSERT INTO `S1` (ID, V0, ROWTIME) VALUES (0, 1, 12);
ASSERT VALUES `OUTPUT` (S1_ID, S1_V0, T2_V0, T3_V0, ROWTIME) VALUES (0, 1, 2, 3, 12);
ASSERT stream OUTPUT (S1_ID INT KEY, S1_V0 BIGINT, T2_V0 BIGINT, T3_V0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-joins - second join column in projection
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT T2.ID, s1.V0, t2.V0, t3.V0 FROM S1 JOIN T2 ON S1.ID = T2.ID JOIN T3 ON S1.ID = T3.ID;
INSERT INTO `T3` (ID, V0, ROWTIME) VALUES (0, 3, 10);
INSERT INTO `T2` (ID, V0, ROWTIME) VALUES (0, 2, 11);
INSERT INTO `S1` (ID, V0, ROWTIME) VALUES (0, 1, 12);
ASSERT VALUES `OUTPUT` (T2_ID, S1_V0, T2_V0, T3_V0, ROWTIME) VALUES (0, 1, 2, 3, 12);
ASSERT stream OUTPUT (T2_ID INT KEY, S1_V0 BIGINT, T2_V0 BIGINT, T3_V0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-joins - third join column in projection
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT T3.ID, s1.V0, t2.V0, t3.V0 FROM S1 JOIN T2 ON S1.ID = T2.ID JOIN T3 ON S1.ID = T3.ID;
INSERT INTO `T3` (ID, V0, ROWTIME) VALUES (0, 3, 10);
INSERT INTO `T2` (ID, V0, ROWTIME) VALUES (0, 2, 11);
INSERT INTO `S1` (ID, V0, ROWTIME) VALUES (0, 1, 12);
ASSERT VALUES `OUTPUT` (T3_ID, S1_V0, T2_V0, T3_V0, ROWTIME) VALUES (0, 1, 2, 3, 12);
ASSERT stream OUTPUT (T3_ID INT KEY, S1_V0 BIGINT, T2_V0 BIGINT, T3_V0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-joins - only viable in projection
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE STREAM S2 (ID INT KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE STREAM S3 (ID INT KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT S1.ID, s1.V0, s2.V0, s3.V0 FROM S1 JOIN S2 WITHIN 10 seconds ON S1.ID = abs(S2.ID) JOIN S3 WITHIN 10 seconds ON abs(S2.ID) = abs(S3.ID);
INSERT INTO `S3` (ID, V0, ROWTIME) VALUES (0, 3, 10);
INSERT INTO `S2` (ID, V0, ROWTIME) VALUES (0, 2, 11);
INSERT INTO `S1` (ID, V0, ROWTIME) VALUES (0, 1, 12);
ASSERT VALUES `OUTPUT` (S1_ID, S1_V0, S2_V0, S3_V0, ROWTIME) VALUES (0, 1, 2, 3, 12);
ASSERT stream OUTPUT (S1_ID INT KEY, S1_V0 BIGINT, S2_V0 BIGINT, S3_V0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-joins - inner-inner - missing join columns in projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the join expressions S1.ID, T2.ID or T3.ID in its projection (eg, SELECT S1.ID...).
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT s1.V0, t2.V0, t3.V0 FROM S1 INNER JOIN T2 ON S1.ID = T2.ID INNER JOIN T3 ON S1.ID = T3.ID;
--@test: multi-joins - inner-inner with expression in first - missing join columns in projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the join expressions S1.ID or T3.ID in its projection (eg, SELECT S1.ID...).
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT s1.V0, t2.V0, t3.V0 FROM S1 INNER JOIN T2 ON S1.ID + 1 = T2.ID INNER JOIN T3 ON S1.ID = T3.ID;
--@test: multi-joins - inner-inner with expression in second - missing join columns in projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the join expression T3.ID in its projection (eg, SELECT T3.ID...).
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT s1.V0, t2.V0, t3.V0 FROM S1 INNER JOIN T2 ON S1.ID = T2.ID INNER JOIN T3 ON S1.ID + 1 = T3.ID;
--@test: multi-joins - left-left - missing join columns in projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the join expressions S1.ID, T2.ID or T3.ID in its projection (eg, SELECT S1.ID...).
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT s1.V0, t2.V0, t3.V0 FROM S1 LEFT JOIN T2 ON S1.ID = T2.ID LEFT JOIN T3 ON S1.ID = T3.ID;
--@test: multi-joins - left-left with expression in first - missing join columns in projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the join expressions S1.ID or T3.ID in its projection (eg, SELECT S1.ID...).
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT s1.V0, t2.V0, t3.V0 FROM S1 LEFT JOIN T2 ON S1.ID + 1 = T2.ID LEFT JOIN T3 ON S1.ID = T3.ID;
--@test: multi-joins - left-left with expression in second - missing join columns in projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the join expression T3.ID in its projection (eg, SELECT T3.ID...).
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT s1.V0, t2.V0, t3.V0 FROM S1 LEFT JOIN T2 ON S1.ID = T2.ID LEFT JOIN T3 ON S1.ID + 1 = T3.ID;
--@test: multi-joins - stream-table-table - table key format mismatch
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left', key_format='JSON', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', key_format='KAFKA', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', key_format='DELIMITED', value_format='JSON');
CREATE STREAM OUTPUT as SELECT S1.ID, s1.V0, t2.V0, t3.V0 FROM S1 JOIN T2 ON S1.ID = T2.ID JOIN T3 ON S1.ID = T3.ID;
INSERT INTO `T3` (ID, V0, ROWTIME) VALUES (0, 3, 10);
INSERT INTO `T2` (ID, V0, ROWTIME) VALUES (0, 2, 11);
INSERT INTO `S1` (ID, V0, ROWTIME) VALUES (0, 1, 12);
INSERT INTO `S1` (ID, V0, ROWTIME) VALUES (1, 1, 14);
ASSERT VALUES `OUTPUT` (S1_ID, S1_V0, T2_V0, T3_V0, ROWTIME) VALUES (0, 1, 2, 3, 12);

--@test: multi-joins - stream-table-table - inner-inner - rekey
CREATE STREAM S1 (ID INT KEY, K INT, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT S1.K, s1.V0, t2.V0, t3.V0 FROM S1 JOIN T2 ON S1.K = T2.ID JOIN T3 ON S1.K = T3.ID;
INSERT INTO `T3` (ID, V0, ROWTIME) VALUES (1, 3, 10);
INSERT INTO `T2` (ID, V0, ROWTIME) VALUES (1, 2, 11);
INSERT INTO `S1` (ID, k, V0, ROWTIME) VALUES (0, 1, 1, 12);
ASSERT VALUES `OUTPUT` (K, S1_V0, T2_V0, T3_V0, ROWTIME) VALUES (1, 1, 2, 3, 12);
ASSERT stream OUTPUT (K INT KEY, S1_V0 BIGINT, T2_V0 BIGINT, T3_V0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-joins - stream-table-table - inner-inner - rekey with different expression
CREATE STREAM S1 (ID INT KEY, K INT, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT T3.ID, s1.V0, t2.V0, t3.V0 FROM S1 JOIN T2 ON S1.K - 1 = T2.ID JOIN T3 ON S1.K + 1 = T3.ID;
INSERT INTO `T3` (ID, V0, ROWTIME) VALUES (3, 3, 10);
INSERT INTO `T2` (ID, V0, ROWTIME) VALUES (1, 2, 11);
INSERT INTO `S1` (ID, k, V0, ROWTIME) VALUES (0, 2, 1, 12);
ASSERT VALUES `OUTPUT` (T3_ID, S1_V0, T2_V0, T3_V0, ROWTIME) VALUES (3, 1, 2, 3, 12);
ASSERT stream OUTPUT (T3_ID INT KEY, S1_V0 BIGINT, T2_V0 BIGINT, T3_V0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-joins - stream-table-table - inner-inner - rekey on different expression
CREATE STREAM S1 (ID INT KEY, K INT, V0 int) WITH (kafka_topic='left', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 int) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 int) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT S1.K, s1.V0, t2.V0, t3.V0 FROM S1 JOIN T2 ON S1.V0 = T2.ID JOIN T3 ON S1.K = T3.ID;
INSERT INTO `T3` (ID, V0, ROWTIME) VALUES (2, 3, 10);
INSERT INTO `T2` (ID, V0, ROWTIME) VALUES (1, 2, 11);
INSERT INTO `S1` (ID, k, V0, ROWTIME) VALUES (0, 2, 1, 12);
ASSERT VALUES `OUTPUT` (K, S1_V0, T2_V0, T3_V0, ROWTIME) VALUES (2, 1, 2, 3, 12);
ASSERT stream OUTPUT (K INT KEY, S1_V0 INT, T2_V0 INT, T3_V0 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-joins - stream-table-table - inner-inner - rekey and flip join expression
CREATE STREAM S1 (ID INT KEY, K INT, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT T3.ID, s1.V0, t2.V0, t3.V0 FROM S1 JOIN T2 ON T2.ID = S1.K JOIN T3 ON T3.ID = S1.K;
INSERT INTO `T3` (ID, V0, ROWTIME) VALUES (1, 3, 10);
INSERT INTO `T2` (ID, V0, ROWTIME) VALUES (1, 2, 11);
INSERT INTO `S1` (ID, k, V0, ROWTIME) VALUES (0, 1, 1, 12);
ASSERT VALUES `OUTPUT` (T3_ID, S1_V0, T2_V0, T3_V0, ROWTIME) VALUES (1, 1, 2, 3, 12);
ASSERT stream OUTPUT (T3_ID INT KEY, S1_V0 BIGINT, T2_V0 BIGINT, T3_V0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-joins - stream-stream-table - inner-inner - rekey left
CREATE STREAM S1 (ID INT KEY, k int, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE STREAM S2 (ID INT KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT S1.K, s1.V0, s2.V0, t3.V0 FROM S1 JOIN S2 WITHIN 10 seconds ON S1.K = S2.ID JOIN T3 ON S1.K = T3.ID;
INSERT INTO `T3` (ID, V0, ROWTIME) VALUES (0, 3, 10);
INSERT INTO `S2` (ID, V0, ROWTIME) VALUES (0, 2, 11);
INSERT INTO `S1` (ID, k, V0, ROWTIME) VALUES (1, 0, 1, 12);
INSERT INTO `S2` (ID, V0, ROWTIME) VALUES (0, 4, 13);
INSERT INTO `S1` (ID, k, V0, ROWTIME) VALUES (1, 0, 5, 100000);
INSERT INTO `S2` (ID, V0, ROWTIME) VALUES (0, 6, 100001);
ASSERT VALUES `OUTPUT` (K, S1_V0, S2_V0, T3_V0, ROWTIME) VALUES (0, 1, 2, 3, 12);
ASSERT VALUES `OUTPUT` (K, S1_V0, S2_V0, T3_V0, ROWTIME) VALUES (0, 1, 4, 3, 13);
ASSERT VALUES `OUTPUT` (K, S1_V0, S2_V0, T3_V0, ROWTIME) VALUES (0, 5, 6, 3, 100001);
ASSERT stream OUTPUT (K INT KEY, S1_V0 BIGINT, S2_V0 BIGINT, T3_V0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-joins - stream-stream-table - inner-inner - rekey left and right
CREATE STREAM S1 (ID INT KEY, k int, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE STREAM S2 (ID INT KEY, k int, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT T3.ID, s1.V0, s2.V0, t3.V0 FROM S1 JOIN S2 WITHIN 10 seconds ON S1.K = S2.K JOIN T3 ON S1.K = T3.ID;
INSERT INTO `T3` (ID, V0, ROWTIME) VALUES (0, 3, 10);
INSERT INTO `S2` (ID, k, V0, ROWTIME) VALUES (2, 0, 2, 11);
INSERT INTO `S1` (ID, k, V0, ROWTIME) VALUES (1, 0, 1, 12);
INSERT INTO `S2` (ID, k, V0, ROWTIME) VALUES (2, 0, 4, 13);
INSERT INTO `S1` (ID, k, V0, ROWTIME) VALUES (1, 0, 5, 100000);
INSERT INTO `S2` (ID, k, V0, ROWTIME) VALUES (2, 0, 6, 100001);
ASSERT VALUES `OUTPUT` (T3_ID, S1_V0, S2_V0, T3_V0, ROWTIME) VALUES (0, 1, 2, 3, 12);
ASSERT VALUES `OUTPUT` (T3_ID, S1_V0, S2_V0, T3_V0, ROWTIME) VALUES (0, 1, 4, 3, 13);
ASSERT VALUES `OUTPUT` (T3_ID, S1_V0, S2_V0, T3_V0, ROWTIME) VALUES (0, 5, 6, 3, 100001);
ASSERT stream OUTPUT (T3_ID INT KEY, S1_V0 BIGINT, S2_V0 BIGINT, T3_V0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-joins - stream-stream-table - inner-inner - cascading join criteria
CREATE STREAM S1 (ID INT KEY, k int, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE STREAM S2 (ID INT KEY, k int, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT s1.k, s1.V0, s2.V0, t3.V0 FROM S1 JOIN S2 WITHIN 10 seconds ON S1.K = S2.K JOIN T3 ON S2.K = T3.ID;
INSERT INTO `T3` (ID, V0, ROWTIME) VALUES (0, 3, 10);
INSERT INTO `S2` (ID, k, V0, ROWTIME) VALUES (2, 0, 2, 11);
INSERT INTO `S1` (ID, k, V0, ROWTIME) VALUES (1, 0, 1, 12);
INSERT INTO `S2` (ID, k, V0, ROWTIME) VALUES (2, 0, 4, 13);
INSERT INTO `S1` (ID, k, V0, ROWTIME) VALUES (1, 0, 5, 100000);
INSERT INTO `S2` (ID, k, V0, ROWTIME) VALUES (2, 0, 6, 100001);
ASSERT VALUES `OUTPUT` (S1_K, S1_V0, S2_V0, T3_V0, ROWTIME) VALUES (0, 1, 2, 3, 12);
ASSERT VALUES `OUTPUT` (S1_K, S1_V0, S2_V0, T3_V0, ROWTIME) VALUES (0, 1, 4, 3, 13);
ASSERT VALUES `OUTPUT` (S1_K, S1_V0, S2_V0, T3_V0, ROWTIME) VALUES (0, 5, 6, 3, 100001);
ASSERT stream OUTPUT (S1_K INT KEY, S1_V0 BIGINT, S2_V0 BIGINT, T3_V0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-joins - stream-stream-table - full-inner
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE STREAM S2 (ID INT KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT S1.ID, s1.V0, s2.V0, t3.V0 FROM S1 FULL JOIN S2 WITHIN 10 seconds ON S1.ID = S2.ID JOIN T3 ON S1.ID = T3.ID;
INSERT INTO `T3` (ID, V0, ROWTIME) VALUES (0, 3, 10);
INSERT INTO `S2` (ID, V0, ROWTIME) VALUES (0, 2, 11);
INSERT INTO `S1` (ID, V0, ROWTIME) VALUES (0, 1, 12);
INSERT INTO `S2` (ID, V0, ROWTIME) VALUES (0, 4, 13);
INSERT INTO `S1` (ID, V0, ROWTIME) VALUES (0, 5, 100000);
INSERT INTO `S2` (ID, V0, ROWTIME) VALUES (0, 6, 100001);
ASSERT VALUES `OUTPUT` (S1_ID, S1_V0, S2_V0, T3_V0, ROWTIME) VALUES (0, 1, 2, 3, 12);
ASSERT VALUES `OUTPUT` (S1_ID, S1_V0, S2_V0, T3_V0, ROWTIME) VALUES (0, 1, 4, 3, 13);
ASSERT VALUES `OUTPUT` (S1_ID, S1_V0, S2_V0, T3_V0, ROWTIME) VALUES (0, 5, NULL, 3, 100000);
ASSERT VALUES `OUTPUT` (S1_ID, S1_V0, S2_V0, T3_V0, ROWTIME) VALUES (0, 5, 6, 3, 100001);
ASSERT stream OUTPUT (S1_ID INT KEY, S1_V0 BIGINT, S2_V0 BIGINT, T3_V0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-joins - stream-stream-table - full-inner - select *
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE STREAM S2 (ID INT KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM S1 FULL JOIN S2 WITHIN 10 seconds ON S1.ID = S2.ID JOIN T3 ON S1.ID = T3.ID;
INSERT INTO `T3` (ID, V0, ROWTIME) VALUES (0, 3, 10);
INSERT INTO `S2` (ID, V0, ROWTIME) VALUES (0, 2, 11);
INSERT INTO `S1` (ID, V0, ROWTIME) VALUES (0, 1, 12);
INSERT INTO `S2` (ID, V0, ROWTIME) VALUES (0, 4, 13);
INSERT INTO `S1` (ID, V0, ROWTIME) VALUES (0, 5, 100000);
INSERT INTO `S2` (ID, V0, ROWTIME) VALUES (0, 6, 100001);
ASSERT VALUES `OUTPUT` (S1_ID, S1_V0, S2_ID, S2_V0, T3_ID, T3_V0, ROWTIME) VALUES (0, 1, 0, 2, 0, 3, 12);
ASSERT VALUES `OUTPUT` (S1_ID, S1_V0, S2_ID, S2_V0, T3_ID, T3_V0, ROWTIME) VALUES (0, 1, 0, 4, 0, 3, 13);
ASSERT VALUES `OUTPUT` (S1_ID, S1_V0, S2_ID, S2_V0, T3_ID, T3_V0, ROWTIME) VALUES (0, 5, NULL, NULL, 0, 3, 100000);
ASSERT VALUES `OUTPUT` (S1_ID, S1_V0, S2_ID, S2_V0, T3_ID, T3_V0, ROWTIME) VALUES (0, 5, 0, 6, 0, 3, 100001);
ASSERT stream OUTPUT (S1_ID INT KEY, S1_V0 BIGINT, S2_ID INT, S2_V0 BIGINT, T3_ID INT, T3_V0 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-joins - table-table-table - key format mismatch
CREATE TABLE T1 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='left', key_format='KAFKA', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', key_format='KAFKA', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', key_format='DELIMITED', value_format='JSON');
CREATE TABLE OUTPUT as SELECT T1.ID, T1.V0, T2.V0, T3.V0 FROM T1 JOIN T2 ON T1.ID = T2.ID JOIN T3 ON T1.ID = T3.ID;
INSERT INTO `T1` (ID, V0, ROWTIME) VALUES (0, 1, 0);
INSERT INTO `T2` (ID, V0, ROWTIME) VALUES (0, 2, 1);
INSERT INTO `T3` (ID, V0, ROWTIME) VALUES (0, 3, 2);
INSERT INTO `T1` (ID, V0, ROWTIME) VALUES (0, 4, 1000);
ASSERT VALUES `OUTPUT` (T1_ID, T1_V0, T2_V0, T3_V0, ROWTIME) VALUES (0, 1, 2, 3, 2);
ASSERT VALUES `OUTPUT` (T1_ID, T1_V0, T2_V0, T3_V0, ROWTIME) VALUES (0, 4, 2, 3, 1000);

--@test: multi-joins - self join with multiple sources
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Each side of the join must reference exactly one source and not the same source. Left side references `S1` and right references `S1`
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM S1 JOIN T2 ON S1.V0 = T2.V0 JOIN T3 ON S1.V0 = S1.V0;
--@test: multi-joins - criteria that does not use the joined source
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: A join criteria is expected to reference the source (`T3`) in the FROM clause, instead the right source references `T2` and the left source references `S1`
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM S1 JOIN T2 ON S1.ID = T2.ID JOIN T3 ON S1.ID = T2.ID;
--@test: multi-joins - table-table-stream - Invalid
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Invalid join order: table-stream joins are not supported; only stream-table joins. Got T2 [INNER] JOIN S1.
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM T2 JOIN T3 ON T2.ID = T3.ID JOIN S1 ON T2.ID = S1.ID;
--@test: multi-joins - join on literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid comparison expression '0' in join '(S1.ID = 0)'. Each side of the join comparision must contain references from exactly one source.
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, DIFF bigint) WITH (kafka_topic='right2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT S1.ID, s1.V0, t2.V0, t3.DIFF FROM S1 JOIN T2 ON S1.ID = 0 JOIN T3 ON S1.ID = T3.ID;
--@test: multi-joins - partition count mismatch
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Can't join `S1` with `T3` since the number of partitions don't match. `S1` partitions = 1; `T3` partitions = 2. Please repartition either one so that the number of partitions match.
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left', value_format='JSON', PARTITIONS=1);
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right', value_format='JSON', PARTITIONS=1);
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right2', value_format='JSON', PARTITIONS=2);
CREATE STREAM OUTPUT as SELECT * FROM S1 JOIN T2 ON S1.ID = T2.ID JOIN T3 ON S1.ID = T3.ID;
--@test: multi-joins - scoped include all columns
CREATE STREAM S1 (A INT KEY, B INT) WITH (kafka_topic='S1', value_format='JSON');
CREATE STREAM S2 (A INT KEY, B INT) WITH (kafka_topic='S2', value_format='JSON');
CREATE STREAM S3 (A INT KEY, B INT) WITH (kafka_topic='S3', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT s1.*, s2.*, s3.* FROM S1 INNER JOIN S2 WITHIN 10 SECONDS ON S1.A = S2.A INNER JOIN S3 WITHIN 10 SECONDS ON S1.A = S3.A WHERE S1.B < 5;
INSERT INTO `S1` (A, B, ROWTIME) VALUES (0, 1, 10);
INSERT INTO `S2` (A, B, ROWTIME) VALUES (0, -1, 11);
INSERT INTO `S3` (A, B, ROWTIME) VALUES (0, 9, 12);
INSERT INTO `S1` (A, B, ROWTIME) VALUES (0, 9, 13);
ASSERT VALUES `OUTPUT` (S1_A, S1_B, S2_A, S2_B, S3_A, S3_B, ROWTIME) VALUES (0, 1, 0, -1, 0, 9, 12);

--@test: multi-joins - duplicate source
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: N-way joins do not support multiple occurrences of the same source. Source: 'T2'
CREATE STREAM S1 (ID INT KEY, V0 bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE T2 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE T3 (ID INT PRIMARY KEY, V0 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM S1 JOIN T2 ON S1.ID = T2.ID JOIN T2 ON S1.V0 = T2.V0;
--@test: multi-joins - stream-table-table - qualified select *
CREATE STREAM left_stream (id1 BIGINT KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE middle_table (id2 BIGINT PRIMARY KEY, f2 BIGINT, other STRING) WITH (kafka_topic='middle_topic', format='JSON');
CREATE TABLE right_table (id3 BIGINT PRIMARY KEY, f3 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE STREAM output AS SELECT id1, f1, middle_table.*, right_table.* FROM left_stream JOIN middle_table ON id1 = id2 LEFT JOIN right_table ON f1 = id3;
INSERT INTO `MIDDLE_TABLE` (ID2, F2, OTHER, ROWTIME) VALUES (0, 100, 'unused', 0);
INSERT INTO `LEFT_STREAM` (ID1, F1, ROWTIME) VALUES (0, 0, 10000);
INSERT INTO `RIGHT_TABLE` (ID3, F3, ROWTIME) VALUES (0, 4, 11000);
INSERT INTO `MIDDLE_TABLE` (ID2, F2, OTHER, ROWTIME) VALUES (8, 10, 'unused', 13000);
INSERT INTO `LEFT_STREAM` (ID1, F1, ROWTIME) VALUES (8, 8, 16000);
INSERT INTO `LEFT_STREAM` (ID1, ROWTIME) VALUES (0, 18000);
ASSERT VALUES `OUTPUT` (F1, ID1, MIDDLE_TABLE_ID2, MIDDLE_TABLE_F2, MIDDLE_TABLE_OTHER, RIGHT_TABLE_ID3, RIGHT_TABLE_F3, ROWTIME) VALUES (0, 0, 0, 100, 'unused', NULL, NULL, 10000);
ASSERT VALUES `OUTPUT` (F1, ID1, MIDDLE_TABLE_ID2, MIDDLE_TABLE_F2, MIDDLE_TABLE_OTHER, RIGHT_TABLE_ID3, RIGHT_TABLE_F3, ROWTIME) VALUES (8, 8, 8, 10, 'unused', NULL, NULL, 16000);
ASSERT stream OUTPUT (F1 BIGINT KEY, ID1 BIGINT, MIDDLE_TABLE_ID2 BIGINT, MIDDLE_TABLE_F2 BIGINT, MIDDLE_TABLE_OTHER STRING, RIGHT_TABLE_ID3 BIGINT, RIGHT_TABLE_F3 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

