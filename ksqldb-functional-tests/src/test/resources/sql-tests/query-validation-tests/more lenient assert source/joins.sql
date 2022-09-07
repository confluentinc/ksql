--@test: joins - aliased synthetic join key
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ROWKEY AS A, l.B, l.C, r.* FROM L INNER JOIN R WITHIN 10 SECONDS ON ABS(L.A) = ABS(R.A);
INSERT INTO `L` (A, B, C, ROWTIME) VALUES (0, 1, 2, 10);
INSERT INTO `R` (A, B, C, ROWTIME) VALUES (0, -1, -2, 11);
ASSERT VALUES `OUTPUT` (A, L_B, L_C, R_A, R_B, R_C, ROWTIME) VALUES (0, 1, 2, 0, -1, -2, 11);
ASSERT stream OUTPUT (A INT KEY, L_B INT, L_C INT, R_A INT, R_B INT, R_C INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - unaliased synthetic join key
CREATE TABLE L (ID INT PRIMARY KEY, V0 INT, V1 INT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE R (ID INT PRIMARY KEY, V0 INT, V1 INT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE OUTPUT as SELECT ROWKEY, L.ID, R.ID, L.V0, R.V1 FROM L FULL OUTER JOIN R on L.id = R.id;
INSERT INTO `L` (ID, V0, V1, ROWTIME) VALUES (1, 2, 3, 0);
INSERT INTO `R` (ID, V0, V1, ROWTIME) VALUES (1, 4, 5, 100);
ASSERT VALUES `OUTPUT` (ROWKEY, L_ID, R_ID, L_V0, R_V1, ROWTIME) VALUES (1, 1, NULL, 2, NULL, 0);
ASSERT VALUES `OUTPUT` (ROWKEY, L_ID, R_ID, L_V0, R_V1, ROWTIME) VALUES (1, 1, 1, 2, 5, 100);
ASSERT table OUTPUT (ROWKEY INT PRIMARY KEY, L_ID INT, R_ID INT, L_V0 INT, R_V1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - inner join - with both join column in projection
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM L INNER JOIN R WITHIN 10 SECONDS ON L.A = R.A;
INSERT INTO `L` (A, B, C, ROWTIME) VALUES (0, 1, 2, 10);
INSERT INTO `R` (A, B, C, ROWTIME) VALUES (0, -1, -2, 11);
ASSERT VALUES `OUTPUT` (L_A, L_B, L_C, R_A, R_B, R_C, ROWTIME) VALUES (0, 1, 2, 0, -1, -2, 11);
ASSERT stream OUTPUT (L_A INT KEY, L_B INT, L_C INT, R_A INT, R_B INT, R_C INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - inner join - with left join column in projection
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT L.*, R.* FROM L INNER JOIN R WITHIN 10 SECONDS ON L.A = ABS(R.A);
INSERT INTO `L` (A, B, C, ROWTIME) VALUES (0, 1, 2, 10);
INSERT INTO `R` (A, B, C, ROWTIME) VALUES (0, -1, -2, 11);
ASSERT VALUES `OUTPUT` (L_A, L_B, L_C, R_A, R_B, R_C, ROWTIME) VALUES (0, 1, 2, 0, -1, -2, 11);
ASSERT stream OUTPUT (L_A INT KEY, L_B INT, L_C INT, R_A INT, R_B INT, R_C INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - inner join - with right join column in projection
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT L.*, R.* FROM L INNER JOIN R WITHIN 10 SECONDS ON ABS(L.A) = R.A;
INSERT INTO `L` (A, B, C, ROWTIME) VALUES (0, 1, 2, 10);
INSERT INTO `R` (A, B, C, ROWTIME) VALUES (0, -1, -2, 11);
ASSERT VALUES `OUTPUT` (R_A, L_A, L_B, L_C, R_B, R_C, ROWTIME) VALUES (0, 0, 1, 2, -1, -2, 11);
ASSERT stream OUTPUT (R_A INT KEY, L_A INT, L_B INT, L_C INT, R_B INT, R_C INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - inner join - with only right join column in projection
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT L.B, L.C, R.* FROM L INNER JOIN R WITHIN 10 SECONDS ON L.A = R.A;
INSERT INTO `L` (A, B, C, ROWTIME) VALUES (0, 1, 2, 10);
INSERT INTO `R` (A, B, C, ROWTIME) VALUES (0, -1, -2, 11);
ASSERT VALUES `OUTPUT` (R_A, L_B, L_C, R_B, R_C, ROWTIME) VALUES (0, 1, 2, -1, -2, 11);
ASSERT stream OUTPUT (R_A INT KEY, L_B INT, L_C INT, R_B INT, R_C INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - inner join - missing join columns in projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the join expressions L.A or R.A in its projection (eg, SELECT L.A...).
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT l.B, l.C, R.B, R.C FROM L INNER JOIN R WITHIN 10 SECONDS ON L.A = R.A;
--@test: joins - inner join - missing synthetic join column in projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Key missing from projection (ie, SELECT). See https://cnfl.io/2LV7ouS.
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT l.*, r.* FROM L INNER JOIN R WITHIN 10 SECONDS ON ABS(L.A) = ABS(R.A);
--@test: joins - left join - with both join column in projection
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM L LEFT JOIN R WITHIN 10 SECONDS ON L.A = R.A;
INSERT INTO `R` (A, B, C, ROWTIME) VALUES (0, -1, -2, 10);
INSERT INTO `L` (A, B, C, ROWTIME) VALUES (0, 1, 2, 10);
ASSERT VALUES `OUTPUT` (L_A, L_B, L_C, R_A, R_B, R_C, ROWTIME) VALUES (0, 1, 2, 0, -1, -2, 10);
ASSERT stream OUTPUT (L_A INT KEY, L_B INT, L_C INT, R_A INT, R_B INT, R_C INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - left join - with left join column in projection
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT L.*, R.* FROM L LEFT JOIN R WITHIN 10 SECONDS ON L.A = ABS(R.A);
INSERT INTO `R` (A, B, C, ROWTIME) VALUES (0, -1, -2, 10);
INSERT INTO `L` (A, B, C, ROWTIME) VALUES (0, 1, 2, 10);
ASSERT VALUES `OUTPUT` (L_A, L_B, L_C, R_A, R_B, R_C, ROWTIME) VALUES (0, 1, 2, 0, -1, -2, 10);
ASSERT stream OUTPUT (L_A INT KEY, L_B INT, L_C INT, R_A INT, R_B INT, R_C INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - left join - with right join column in projection
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT L.*, R.* FROM L LEFT JOIN R WITHIN 10 SECONDS ON ABS(L.A) = R.A;
INSERT INTO `R` (A, B, C, ROWTIME) VALUES (0, -1, -2, 10);
INSERT INTO `L` (A, B, C, ROWTIME) VALUES (0, 1, 2, 10);
ASSERT VALUES `OUTPUT` (R_A, L_A, L_B, L_C, R_B, R_C, ROWTIME) VALUES (0, 0, 1, 2, -1, -2, 10);
ASSERT stream OUTPUT (R_A INT KEY, L_A INT, L_B INT, L_C INT, R_B INT, R_C INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - left join - with synthetic join column in projection
CREATE STREAM L (ID INT KEY, V0 INT, V1 INT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM R (ID INT KEY, V0 INT, V1 INT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT ROWKEY AS ID, L.ID, R.ID, L.V0, R.V1 FROM L LEFT JOIN R WITHIN 1 SECOND ON ABS(L.id) = ABS(R.id);
INSERT INTO `L` (ID, V0, V1, ROWTIME) VALUES (1, 2, 3, 0);
INSERT INTO `R` (ID, V0, V1, ROWTIME) VALUES (1, 4, 5, 100);
ASSERT VALUES `OUTPUT` (ID, L_ID, R_ID, L_V0, R_V1, ROWTIME) VALUES (1, 1, NULL, 2, NULL, 0);
ASSERT VALUES `OUTPUT` (ID, L_ID, R_ID, L_V0, R_V1, ROWTIME) VALUES (1, 1, 1, 2, 5, 100);
ASSERT stream OUTPUT (ID INT KEY, L_ID INT, R_ID INT, L_V0 INT, R_V1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - left join - missing join columns in projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the join expressions L.A or R.A in its projection (eg, SELECT L.A...).
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT l.B, l.C, r.B, r.C FROM L LEFT JOIN R WITHIN 10 SECONDS ON L.A = R.A;
--@test: joins - left join - missing join columns in projection - with star
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the join expression R.A in its projection (eg, SELECT R.A...).
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT l.*, r.B, r.C FROM L LEFT JOIN R WITHIN 10 SECONDS ON ABS(L.A) = R.A;
--@test: joins - left join - missing synthetic join column in projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Key missing from projection (ie, SELECT). See https://cnfl.io/2LV7ouS.
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT l.*, r.* FROM L LEFT JOIN R WITHIN 10 SECONDS ON ABS(L.A) = ABS(R.A);
--@test: joins - full join - with both join column in projection
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM L FULL OUTER JOIN R WITHIN 10 SECONDS ON L.A = R.A;
INSERT INTO `R` (A, B, C, ROWTIME) VALUES (0, -1, -2, 9);
INSERT INTO `L` (A, B, C, ROWTIME) VALUES (0, 1, 2, 10);
ASSERT VALUES `OUTPUT` (ROWKEY, L_A, L_B, L_C, R_A, R_B, R_C, ROWTIME) VALUES (0, NULL, NULL, NULL, 0, -1, -2, 9);
ASSERT VALUES `OUTPUT` (ROWKEY, L_A, L_B, L_C, R_A, R_B, R_C, ROWTIME) VALUES (0, 0, 1, 2, 0, -1, -2, 10);
ASSERT stream OUTPUT (ROWKEY INT KEY, L_A INT, L_B INT, L_C INT, R_A INT, R_B INT, R_C INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - full join - missing synthetic join column in projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the join expression ROWKEY in its projection (eg, SELECT ROWKEY...).
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT l.B, l.C, r.* FROM L FULL OUTER JOIN R WITHIN 10 SECONDS ON L.A = R.A;
--@test: joins - missing join columns in projection - with value column of same name
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the join expressions L.A or R.A in its projection (eg, SELECT L.A...).
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT l.B AS L_A, l.C, R.B AS R_A, R.C FROM L JOIN R WITHIN 10 SECONDS ON L.A = R.A;
--@test: joins - key in projection more than once
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The projection contains a key column (`L_A`) more than once, aliased as: KEY2 and L_A.
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT l.A, l.A AS KEY2, l.C, R.C FROM L JOIN R WITHIN 10 SECONDS ON L.A = R.A;
--@test: joins - stream stream left join - KAFKA
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Source(s) S_LEFT, S_RIGHT are using the 'KAFKA' value format. This format does not yet support JOIN.
CREATE STREAM S_LEFT (ID BIGINT KEY, NAME varchar) WITH (kafka_topic='left_topic', value_format='KAFKA');
CREATE STREAM S_RIGHT (ID BIGINT KEY, NAME varchar) WITH (kafka_topic='right_topic', value_format='KAFKA');
CREATE STREAM OUTPUT WITH(value_format='delimited') as SELECT * FROM s_left LEFT JOIN s_right WITHIN 1 second ON s_left.id = s_right.id;
--@test: joins - stream stream left join with key in projection - rekey - AVRO
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT t.id, t.k, name, value, f1, f2 FROM test t left join TEST_STREAM tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'blah', 50, 10000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 10, '100', 5, 11000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 100, 'newblah', 150, 16000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 90, 'ninety', 90, 17000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'zero', 0, NULL, NULL, 0);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (10, 'foo', '100', 5, NULL, NULL, 11000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (90, 'foo', 'ninety', 90, NULL, NULL, 17000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'bar', 99, NULL, NULL, 30000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, T_K STRING, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream left join with key in projection - rekey - JSON
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT t.id, t.k, name, value, f1, f2 FROM test t left join TEST_STREAM tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'blah', 50, 10000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 10, '100', 5, 11000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 100, 'newblah', 150, 16000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 90, 'ninety', 90, 17000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'zero', 0, NULL, NULL, 0);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (10, 'foo', '100', 5, NULL, NULL, 11000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (90, 'foo', 'ninety', 90, NULL, NULL, 17000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'bar', 99, NULL, NULL, 30000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, T_K STRING, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream left join with key in projection - rekey - PROTOBUF
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT as SELECT t.id, t.k, name, value, f1, f2 FROM test t left join TEST_STREAM tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'blah', 50, 10000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 10, '100', 5, 11000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 100, 'newblah', 150, 16000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 90, 'ninety', 90, 17000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'zero', 0, '', 0, 0);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (10, 'foo', '100', 5, '', 0, 11000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (90, 'foo', 'ninety', 90, '', 0, 17000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'bar', 99, '', 0, 30000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, T_K STRING, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream left join with key in projection - rekey - PROTOBUF_NOSR
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT as SELECT t.id, t.k, name, value, f1, f2 FROM test t left join TEST_STREAM tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'blah', 50, 10000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 10, '100', 5, 11000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 100, 'newblah', 150, 16000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 90, 'ninety', 90, 17000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'zero', 0, '', 0, 0);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (10, 'foo', '100', 5, '', 0, 11000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (90, 'foo', 'ninety', 90, '', 0, 17000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'bar', 99, '', 0, 30000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, T_K STRING, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream left join - rekey - AVRO
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT t.id, name, value, f1, f2 FROM test t left join TEST_STREAM tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, NULL, NULL, 0);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (10, '100', 5, NULL, NULL, 11000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (90, 'ninety', 90, NULL, NULL, 17000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'bar', 99, NULL, NULL, 30000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream left join - rekey - JSON
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT t.id, name, value, f1, f2 FROM test t left join TEST_STREAM tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, NULL, NULL, 0);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (10, '100', 5, NULL, NULL, 11000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (90, 'ninety', 90, NULL, NULL, 17000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'bar', 99, NULL, NULL, 30000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream left join - rekey - PROTOBUF
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT as SELECT t.id, name, value, f1, f2 FROM test t left join TEST_STREAM tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, '', 0, 0);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (10, '100', 5, '', 0, 11000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (90, 'ninety', 90, '', 0, 17000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'bar', 99, '', 0, 30000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream left join - rekey - PROTOBUF_NOSR
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT as SELECT t.id, name, value, f1, f2 FROM test t left join TEST_STREAM tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, '', 0, 0);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (10, '100', 5, '', 0, 11000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (90, 'ninety', 90, '', 0, 17000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'bar', 99, '', 0, 30000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream right join - KAFKA
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Source(s) S_LEFT, S_RIGHT are using the 'KAFKA' value format. This format does not yet support JOIN.
CREATE STREAM S_LEFT (ID BIGINT KEY, NAME varchar) WITH (kafka_topic='left_topic', value_format='KAFKA');
CREATE STREAM S_RIGHT (ID BIGINT KEY, NAME varchar) WITH (kafka_topic='right_topic', value_format='KAFKA');
CREATE STREAM OUTPUT WITH(value_format='delimited') as SELECT * FROM s_left RIGHT JOIN s_right WITHIN 1 second ON s_left.id = s_right.id;
--@test: joins - stream stream right join with key in projection - rekey - AVRO
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT t.id, t.k, name, value, f1, f2 FROM test t RIGHT JOIN test_stream tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'blah', 50, 10000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 10, '100', 5, 11000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 100, 'newblah', 150, 16000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 90, 'ninety', 90, 17000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (100, NULL, NULL, NULL, 'newblah', 150, 16000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, T_K STRING, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream right join with key in projection - rekey - JSON
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT t.id, t.k, name, value, f1, f2 FROM test t RIGHT JOIN test_stream tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'blah', 50, 10000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 10, '100', 5, 11000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 100, 'newblah', 150, 16000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 90, 'ninety', 90, 17000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (100, NULL, NULL, NULL, 'newblah', 150, 16000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, T_K STRING, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream right join with key in projection - rekey - PROTOBUF
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT as SELECT t.id, t.k, name, value, f1, f2 FROM test t RIGHT JOIN test_stream tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'blah', 50, 10000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 10, '100', 5, 11000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 100, 'newblah', 150, 16000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 90, 'ninety', 90, 17000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (100, '', '', 0, 'newblah', 150, 16000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, T_K STRING, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream right join with key in projection - rekey - PROTOBUF_NOSR
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT as SELECT t.id, t.k, name, value, f1, f2 FROM test t RIGHT JOIN test_stream tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'blah', 50, 10000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 10, '100', 5, 11000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (K, ID, F1, F2, ROWTIME) VALUES ('foo', 100, 'newblah', 150, 16000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 90, 'ninety', 90, 17000);
INSERT INTO `TEST` (K, ID, NAME, VALUE, ROWTIME) VALUES ('foo', 0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, T_K, NAME, VALUE, F1, F2, ROWTIME) VALUES (100, '', '', 0, 'newblah', 150, 16000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, T_K STRING, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream right join - rekey - AVRO
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT t.id, name, value, f1, f2 FROM test t RIGHT JOIN test_stream tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (100, NULL, NULL, 'newblah', 150, 16000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream right join - rekey - JSON
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT t.id, name, value, f1, f2 FROM test t RIGHT JOIN test_stream tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (100, NULL, NULL, 'newblah', 150, 16000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream right join - rekey - PROTOBUF
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT as SELECT t.id, name, value, f1, f2 FROM test t RIGHT JOIN test_stream tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (100, '', 0, 'newblah', 150, 16000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream right join - rekey - PROTOBUF_NOSR
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM TEST_STREAM (K STRING KEY, ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT as SELECT t.id, name, value, f1, f2 FROM test t RIGHT JOIN test_stream tt WITHIN 11 seconds ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 50, 10000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (100, '', 0, 'newblah', 150, 16000);
ASSERT stream OUTPUT (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream inner join all left fields some right - AVRO
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM INNER_JOIN as SELECT t.*, tt.f1 FROM test t inner join TEST_STREAM tt WITHIN 11 SECONDS ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `INNER_JOIN` (T_ID, T_NAME, T_VALUE, F1, ROWTIME) VALUES (0, 'zero', 0, 'blah', 10000);
ASSERT VALUES `INNER_JOIN` (T_ID, T_NAME, T_VALUE, F1, ROWTIME) VALUES (0, 'foo', 100, 'blah', 13000);
ASSERT VALUES `INNER_JOIN` (T_ID, T_NAME, T_VALUE, F1, ROWTIME) VALUES (0, 'foo', 100, 'a', 15000);
ASSERT stream INNER_JOIN (T_ID BIGINT KEY, T_NAME STRING, T_VALUE BIGINT, F1 STRING) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join all left fields some right - JSON
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM INNER_JOIN as SELECT t.*, tt.f1 FROM test t inner join TEST_STREAM tt WITHIN 11 SECONDS ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `INNER_JOIN` (T_ID, T_NAME, T_VALUE, F1, ROWTIME) VALUES (0, 'zero', 0, 'blah', 10000);
ASSERT VALUES `INNER_JOIN` (T_ID, T_NAME, T_VALUE, F1, ROWTIME) VALUES (0, 'foo', 100, 'blah', 13000);
ASSERT VALUES `INNER_JOIN` (T_ID, T_NAME, T_VALUE, F1, ROWTIME) VALUES (0, 'foo', 100, 'a', 15000);
ASSERT stream INNER_JOIN (T_ID BIGINT KEY, T_NAME STRING, T_VALUE BIGINT, F1 STRING) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join all left fields some right - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF');
CREATE STREAM INNER_JOIN as SELECT t.*, tt.f1 FROM test t inner join TEST_STREAM tt WITHIN 11 SECONDS ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `INNER_JOIN` (T_ID, T_NAME, T_VALUE, F1, ROWTIME) VALUES (0, 'zero', 0, 'blah', 10000);
ASSERT VALUES `INNER_JOIN` (T_ID, T_NAME, T_VALUE, F1, ROWTIME) VALUES (0, 'foo', 100, 'blah', 13000);
ASSERT VALUES `INNER_JOIN` (T_ID, T_NAME, T_VALUE, F1, ROWTIME) VALUES (0, 'foo', 100, 'a', 15000);
ASSERT stream INNER_JOIN (T_ID BIGINT KEY, T_NAME STRING, T_VALUE BIGINT, F1 STRING) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join all left fields some right - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM INNER_JOIN as SELECT t.*, tt.f1 FROM test t inner join TEST_STREAM tt WITHIN 11 SECONDS ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `INNER_JOIN` (T_ID, T_NAME, T_VALUE, F1, ROWTIME) VALUES (0, 'zero', 0, 'blah', 10000);
ASSERT VALUES `INNER_JOIN` (T_ID, T_NAME, T_VALUE, F1, ROWTIME) VALUES (0, 'foo', 100, 'blah', 13000);
ASSERT VALUES `INNER_JOIN` (T_ID, T_NAME, T_VALUE, F1, ROWTIME) VALUES (0, 'foo', 100, 'a', 15000);
ASSERT stream INNER_JOIN (T_ID BIGINT KEY, T_NAME STRING, T_VALUE BIGINT, F1 STRING) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join all right fields some left - AVRO
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM INNER_JOIN as SELECT t.*, tt.name, tt.id FROM test tt inner join TEST_STREAM t WITHIN 11 SECONDS ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, T_F2, NAME, ROWTIME) VALUES (0, 0, 'blah', 50, 'zero', 10000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, T_F2, NAME, ROWTIME) VALUES (0, 0, 'blah', 50, 'foo', 13000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, T_F2, NAME, ROWTIME) VALUES (0, 0, 'a', 10, 'foo', 15000);
ASSERT stream INNER_JOIN (TT_ID BIGINT KEY, T_ID BIGINT, T_F1 STRING, T_F2 BIGINT, NAME STRING) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join all right fields some left - JSON
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM INNER_JOIN as SELECT t.*, tt.name, tt.id FROM test tt inner join TEST_STREAM t WITHIN 11 SECONDS ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, T_F2, NAME, ROWTIME) VALUES (0, 0, 'blah', 50, 'zero', 10000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, T_F2, NAME, ROWTIME) VALUES (0, 0, 'blah', 50, 'foo', 13000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, T_F2, NAME, ROWTIME) VALUES (0, 0, 'a', 10, 'foo', 15000);
ASSERT stream INNER_JOIN (TT_ID BIGINT KEY, T_ID BIGINT, T_F1 STRING, T_F2 BIGINT, NAME STRING) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join all right fields some left - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF');
CREATE STREAM INNER_JOIN as SELECT t.*, tt.name, tt.id FROM test tt inner join TEST_STREAM t WITHIN 11 SECONDS ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, T_F2, NAME, ROWTIME) VALUES (0, 0, 'blah', 50, 'zero', 10000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, T_F2, NAME, ROWTIME) VALUES (0, 0, 'blah', 50, 'foo', 13000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, T_F2, NAME, ROWTIME) VALUES (0, 0, 'a', 10, 'foo', 15000);
ASSERT stream INNER_JOIN (TT_ID BIGINT KEY, T_ID BIGINT, T_F1 STRING, T_F2 BIGINT, NAME STRING) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join all right fields some left - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM INNER_JOIN as SELECT t.*, tt.name, tt.id FROM test tt inner join TEST_STREAM t WITHIN 11 SECONDS ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, T_F2, NAME, ROWTIME) VALUES (0, 0, 'blah', 50, 'zero', 10000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, T_F2, NAME, ROWTIME) VALUES (0, 0, 'blah', 50, 'foo', 13000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, T_F2, NAME, ROWTIME) VALUES (0, 0, 'a', 10, 'foo', 15000);
ASSERT stream INNER_JOIN (TT_ID BIGINT KEY, T_ID BIGINT, T_F1 STRING, T_F2 BIGINT, NAME STRING) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join with stars and duplicates
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM INNER_JOIN as SELECT t.*, t.F1 AS F1_2, tt.*, tt.NAME AS NAME_2 FROM test tt inner join TEST_STREAM t WITHIN 11 SECONDS ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, T_F2, TT_NAME, TT_VALUE, F1_2, NAME_2, ROWTIME) VALUES (0, 0, 'blah', 50, 'zero', 0, 'blah', 'zero', 10000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, T_F2, TT_NAME, TT_VALUE, F1_2, NAME_2, ROWTIME) VALUES (0, 0, 'blah', 50, 'foo', 100, 'blah', 'foo', 13000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, T_F2, TT_NAME, TT_VALUE, F1_2, NAME_2, ROWTIME) VALUES (0, 0, 'a', 10, 'foo', 100, 'a', 'foo', 15000);
ASSERT stream INNER_JOIN (TT_ID BIGINT KEY, T_ID BIGINT, T_F1 STRING, T_F2 BIGINT, F1_2 STRING, TT_NAME STRING, TT_VALUE BIGINT, NAME_2 STRING) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join all fields - AVRO
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM INNER_JOIN as SELECT * FROM test tt inner join TEST_STREAM t WITHIN 11 SECONDS ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (0, 'zero', 0);
INSERT INTO `TEST_STREAM` (ID, F1, ROWTIME) VALUES (0, 'blah', 10000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (10, '100', 11000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (0, 'foo', 13000);
INSERT INTO `TEST_STREAM` (ID, F1, ROWTIME) VALUES (0, 'a', 15000);
INSERT INTO `TEST_STREAM` (ID, F1, ROWTIME) VALUES (100, 'newblah', 16000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (90, 'ninety', 17000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (0, 'bar', 30000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, TT_NAME, ROWTIME) VALUES (0, 0, 'blah', 'zero', 10000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, TT_NAME, ROWTIME) VALUES (0, 0, 'blah', 'foo', 13000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, TT_NAME, ROWTIME) VALUES (0, 0, 'a', 'foo', 15000);
ASSERT stream INNER_JOIN (TT_ID BIGINT KEY, TT_NAME STRING, T_ID BIGINT, T_F1 STRING) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join all fields - JSON
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM INNER_JOIN as SELECT * FROM test tt inner join TEST_STREAM t WITHIN 11 SECONDS ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (0, 'zero', 0);
INSERT INTO `TEST_STREAM` (ID, F1, ROWTIME) VALUES (0, 'blah', 10000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (10, '100', 11000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (0, 'foo', 13000);
INSERT INTO `TEST_STREAM` (ID, F1, ROWTIME) VALUES (0, 'a', 15000);
INSERT INTO `TEST_STREAM` (ID, F1, ROWTIME) VALUES (100, 'newblah', 16000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (90, 'ninety', 17000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (0, 'bar', 30000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, TT_NAME, ROWTIME) VALUES (0, 0, 'blah', 'zero', 10000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, TT_NAME, ROWTIME) VALUES (0, 0, 'blah', 'foo', 13000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, TT_NAME, ROWTIME) VALUES (0, 0, 'a', 'foo', 15000);
ASSERT stream INNER_JOIN (TT_ID BIGINT KEY, TT_NAME STRING, T_ID BIGINT, T_F1 STRING) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join all fields - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar) WITH (kafka_topic='left_topic', value_format='PROTOBUF');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar) WITH (kafka_topic='right_topic', value_format='PROTOBUF');
CREATE STREAM INNER_JOIN as SELECT * FROM test tt inner join TEST_STREAM t WITHIN 11 SECONDS ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (0, 'zero', 0);
INSERT INTO `TEST_STREAM` (ID, F1, ROWTIME) VALUES (0, 'blah', 10000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (10, '100', 11000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (0, 'foo', 13000);
INSERT INTO `TEST_STREAM` (ID, F1, ROWTIME) VALUES (0, 'a', 15000);
INSERT INTO `TEST_STREAM` (ID, F1, ROWTIME) VALUES (100, 'newblah', 16000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (90, 'ninety', 17000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (0, 'bar', 30000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, TT_NAME, ROWTIME) VALUES (0, 0, 'blah', 'zero', 10000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, TT_NAME, ROWTIME) VALUES (0, 0, 'blah', 'foo', 13000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, TT_NAME, ROWTIME) VALUES (0, 0, 'a', 'foo', 15000);
ASSERT stream INNER_JOIN (TT_ID BIGINT KEY, TT_NAME STRING, T_ID BIGINT, T_F1 STRING) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join all fields - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar) WITH (kafka_topic='left_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar) WITH (kafka_topic='right_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM INNER_JOIN as SELECT * FROM test tt inner join TEST_STREAM t WITHIN 11 SECONDS ON t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (0, 'zero', 0);
INSERT INTO `TEST_STREAM` (ID, F1, ROWTIME) VALUES (0, 'blah', 10000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (10, '100', 11000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (0, 'foo', 13000);
INSERT INTO `TEST_STREAM` (ID, F1, ROWTIME) VALUES (0, 'a', 15000);
INSERT INTO `TEST_STREAM` (ID, F1, ROWTIME) VALUES (100, 'newblah', 16000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (90, 'ninety', 17000);
INSERT INTO `TEST` (ID, NAME, ROWTIME) VALUES (0, 'bar', 30000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, TT_NAME, ROWTIME) VALUES (0, 0, 'blah', 'zero', 10000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, TT_NAME, ROWTIME) VALUES (0, 0, 'blah', 'foo', 13000);
ASSERT VALUES `INNER_JOIN` (TT_ID, T_ID, T_F1, TT_NAME, ROWTIME) VALUES (0, 0, 'a', 'foo', 15000);
ASSERT stream INNER_JOIN (TT_ID BIGINT KEY, TT_NAME STRING, T_ID BIGINT, T_F1 STRING) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join with different before and after windows - AVRO
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM INNER_JOIN as SELECT t.id, name, value, f1, f2 FROM test t join TEST_STREAM tt WITHIN (11 seconds, 10 seconds) on t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 12000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT stream INNER_JOIN (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join with different before and after windows - JSON
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM INNER_JOIN as SELECT t.id, name, value, f1, f2 FROM test t join TEST_STREAM tt WITHIN (11 seconds, 10 seconds) on t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 12000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT stream INNER_JOIN (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join with different before and after windows - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF');
CREATE STREAM INNER_JOIN as SELECT t.id, name, value, f1, f2 FROM test t join TEST_STREAM tt WITHIN (11 seconds, 10 seconds) on t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 12000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT stream INNER_JOIN (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join with different before and after windows - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM INNER_JOIN as SELECT t.id, name, value, f1, f2 FROM test t join TEST_STREAM tt WITHIN (11 seconds, 10 seconds) on t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 12000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT stream INNER_JOIN (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join with out of order messages - AVRO
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM INNER_JOIN as SELECT t.id, name, value, f1, f2 FROM test t join TEST_STREAM tt WITHIN 10 seconds on t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 9999);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'late-message', 10000, 6000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 50, 9999);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'late-message', 10000, 'blah', 50, 9999);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'late-message', 10000, 'a', 10, 15000);
ASSERT stream INNER_JOIN (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join with out of order messages - JSON
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM INNER_JOIN as SELECT t.id, name, value, f1, f2 FROM test t join TEST_STREAM tt WITHIN 10 seconds on t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 9999);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'late-message', 10000, 6000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 50, 9999);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'late-message', 10000, 'blah', 50, 9999);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'late-message', 10000, 'a', 10, 15000);
ASSERT stream INNER_JOIN (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join with out of order messages - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF');
CREATE STREAM INNER_JOIN as SELECT t.id, name, value, f1, f2 FROM test t join TEST_STREAM tt WITHIN 10 seconds on t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 9999);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'late-message', 10000, 6000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 50, 9999);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'late-message', 10000, 'blah', 50, 9999);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'late-message', 10000, 'a', 10, 15000);
ASSERT stream INNER_JOIN (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join with out of order messages - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM TEST_STREAM (ID BIGINT KEY, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM INNER_JOIN as SELECT t.id, name, value, f1, f2 FROM test t join TEST_STREAM tt WITHIN 10 seconds on t.id = tt.id;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 50, 9999);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (10, '100', 5, 11000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST_STREAM` (ID, F1, F2, ROWTIME) VALUES (100, 'newblah', 150, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 30000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'late-message', 10000, 6000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 50, 9999);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'blah', 50, 13000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'foo', 100, 'a', 10, 15000);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'late-message', 10000, 'blah', 50, 9999);
ASSERT VALUES `INNER_JOIN` (T_ID, NAME, VALUE, F1, F2, ROWTIME) VALUES (0, 'late-message', 10000, 'a', 10, 15000);
ASSERT stream INNER_JOIN (T_ID BIGINT KEY, NAME STRING, VALUE BIGINT, F1 STRING, F2 BIGINT) WITH (KAFKA_TOPIC='INNER_JOIN');

--@test: joins - stream stream inner join with out of order and custom grace period - AVRO
CREATE STREAM LEFT_STREAM (id BIGINT KEY, l1 VARCHAR) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM RIGHT_STREAM (id BIGINT KEY, l2 VARCHAR) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM INNER_JOIN as SELECT t.id, l1, l2 FROM LEFT_STREAM t join RIGHT_STREAM tt WITHIN 1 minute GRACE PERIOD 1 minute on t.id = tt.id;
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (0, 'A', 0);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (0, 'a', 60000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (1, 'B', 330000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (2, 'C', 90000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (2, 'c', 90000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (3, 'D', 60000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (3, 'd', 60000);
ASSERT VALUES `INNER_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (0, 'A', 'a', 60000);
ASSERT VALUES `INNER_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (2, 'C', 'c', 90000);

--@test: joins - stream stream inner join with out of order and custom grace period - JSON
CREATE STREAM LEFT_STREAM (id BIGINT KEY, l1 VARCHAR) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM RIGHT_STREAM (id BIGINT KEY, l2 VARCHAR) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM INNER_JOIN as SELECT t.id, l1, l2 FROM LEFT_STREAM t join RIGHT_STREAM tt WITHIN 1 minute GRACE PERIOD 1 minute on t.id = tt.id;
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (0, 'A', 0);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (0, 'a', 60000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (1, 'B', 330000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (2, 'C', 90000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (2, 'c', 90000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (3, 'D', 60000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (3, 'd', 60000);
ASSERT VALUES `INNER_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (0, 'A', 'a', 60000);
ASSERT VALUES `INNER_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (2, 'C', 'c', 90000);

--@test: joins - stream stream inner join with out of order and custom grace period - PROTOBUF
CREATE STREAM LEFT_STREAM (id BIGINT KEY, l1 VARCHAR) WITH (kafka_topic='left_topic', value_format='PROTOBUF');
CREATE STREAM RIGHT_STREAM (id BIGINT KEY, l2 VARCHAR) WITH (kafka_topic='right_topic', value_format='PROTOBUF');
CREATE STREAM INNER_JOIN as SELECT t.id, l1, l2 FROM LEFT_STREAM t join RIGHT_STREAM tt WITHIN 1 minute GRACE PERIOD 1 minute on t.id = tt.id;
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (0, 'A', 0);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (0, 'a', 60000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (1, 'B', 330000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (2, 'C', 90000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (2, 'c', 90000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (3, 'D', 60000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (3, 'd', 60000);
ASSERT VALUES `INNER_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (0, 'A', 'a', 60000);
ASSERT VALUES `INNER_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (2, 'C', 'c', 90000);

--@test: joins - stream stream inner join with out of order and custom grace period - PROTOBUF_NOSR
CREATE STREAM LEFT_STREAM (id BIGINT KEY, l1 VARCHAR) WITH (kafka_topic='left_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM RIGHT_STREAM (id BIGINT KEY, l2 VARCHAR) WITH (kafka_topic='right_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM INNER_JOIN as SELECT t.id, l1, l2 FROM LEFT_STREAM t join RIGHT_STREAM tt WITHIN 1 minute GRACE PERIOD 1 minute on t.id = tt.id;
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (0, 'A', 0);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (0, 'a', 60000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (1, 'B', 330000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (2, 'C', 90000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (2, 'c', 90000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (3, 'D', 60000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (3, 'd', 60000);
ASSERT VALUES `INNER_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (0, 'A', 'a', 60000);
ASSERT VALUES `INNER_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (2, 'C', 'c', 90000);

--@test: joins - stream stream left join with out of order and custom grace period - AVRO
CREATE STREAM LEFT_STREAM (id BIGINT KEY, l1 VARCHAR) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM RIGHT_STREAM (id BIGINT KEY, l2 VARCHAR) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM LEFT_JOIN as SELECT t.id, l1, l2 FROM LEFT_STREAM t left join RIGHT_STREAM tt WITHIN 1 minute GRACE PERIOD 1 minute on t.id = tt.id;
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (0, 'A', 0);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (0, 'a', 60000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (1, 'B', 330000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (2, 'C', 90000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (2, 'c', 90000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (3, 'D', 60000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (3, 'd', 60000);
ASSERT VALUES `LEFT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (0, 'A', 'a', 60000);
ASSERT VALUES `LEFT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (2, 'C', NULL, 90000);
ASSERT VALUES `LEFT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (2, 'C', 'c', 90000);
ASSERT VALUES `LEFT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (3, 'D', NULL, 60000);

--@test: joins - stream stream left join with out of order and custom grace period - JSON
CREATE STREAM LEFT_STREAM (id BIGINT KEY, l1 VARCHAR) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM RIGHT_STREAM (id BIGINT KEY, l2 VARCHAR) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM LEFT_JOIN as SELECT t.id, l1, l2 FROM LEFT_STREAM t left join RIGHT_STREAM tt WITHIN 1 minute GRACE PERIOD 1 minute on t.id = tt.id;
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (0, 'A', 0);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (0, 'a', 60000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (1, 'B', 330000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (2, 'C', 90000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (2, 'c', 90000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (3, 'D', 60000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (3, 'd', 60000);
ASSERT VALUES `LEFT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (0, 'A', 'a', 60000);
ASSERT VALUES `LEFT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (2, 'C', NULL, 90000);
ASSERT VALUES `LEFT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (2, 'C', 'c', 90000);
ASSERT VALUES `LEFT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (3, 'D', NULL, 60000);

--@test: joins - stream stream right join with out of order and custom grace period - AVRO
CREATE STREAM LEFT_STREAM (id BIGINT KEY, l1 VARCHAR) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM RIGHT_STREAM (id BIGINT KEY, l2 VARCHAR) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM RIGHT_JOIN as SELECT t.id, l1, l2 FROM LEFT_STREAM t RIGHT JOIN RIGHT_STREAM tt WITHIN 1 minute GRACE PERIOD 1 minute on t.id = tt.id;
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (0, 'A', 0);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (0, 'a', 60000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (1, 'B', 330000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (2, 'C', 90000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (2, 'c', 90000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (3, 'd', 60000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (3, 'D', 90000);
ASSERT VALUES `RIGHT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (0, 'A', 'a', 60000);
ASSERT VALUES `RIGHT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (2, 'C', 'c', 90000);
ASSERT VALUES `RIGHT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (3, NULL, 'd', 60000);
ASSERT VALUES `RIGHT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (3, 'D', 'd', 90000);

--@test: joins - stream stream right join with out of order and custom grace period - JSON
CREATE STREAM LEFT_STREAM (id BIGINT KEY, l1 VARCHAR) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM RIGHT_STREAM (id BIGINT KEY, l2 VARCHAR) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM RIGHT_JOIN as SELECT t.id, l1, l2 FROM LEFT_STREAM t RIGHT JOIN RIGHT_STREAM tt WITHIN 1 minute GRACE PERIOD 1 minute on t.id = tt.id;
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (0, 'A', 0);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (0, 'a', 60000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (1, 'B', 330000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (2, 'C', 90000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (2, 'c', 90000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (3, 'd', 60000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (3, 'D', 90000);
ASSERT VALUES `RIGHT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (0, 'A', 'a', 60000);
ASSERT VALUES `RIGHT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (2, 'C', 'c', 90000);
ASSERT VALUES `RIGHT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (3, NULL, 'd', 60000);
ASSERT VALUES `RIGHT_JOIN` (T_ID, L1, L2, ROWTIME) VALUES (3, 'D', 'd', 90000);

--@test: joins - stream stream full outer join with out of order and custom grace period - AVRO
CREATE STREAM LEFT_STREAM (id BIGINT KEY, l1 VARCHAR) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM RIGHT_STREAM (id BIGINT KEY, l2 VARCHAR) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM OUTER_JOIN as SELECT ROWKEY as ID, t.id, tt.id, l1, l2 FROM LEFT_STREAM t full outer join RIGHT_STREAM tt WITHIN 1 minute GRACE PERIOD 1 minute on t.id = tt.id;
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (0, 'A', 0);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (0, 'a', 60000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (1, 'b', 330000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (2, 'c', 90000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (2, 'C', 90000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (3, 'd', 60000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (3, 'D', 60000);
ASSERT VALUES `OUTER_JOIN` (ID, T_ID, TT_ID, L1, L2, ROWTIME) VALUES (0, 0, 0, 'A', 'a', 60000);
ASSERT VALUES `OUTER_JOIN` (ID, T_ID, TT_ID, L1, L2, ROWTIME) VALUES (2, NULL, 2, NULL, 'c', 90000);
ASSERT VALUES `OUTER_JOIN` (ID, T_ID, TT_ID, L1, L2, ROWTIME) VALUES (2, 2, 2, 'C', 'c', 90000);
ASSERT VALUES `OUTER_JOIN` (ID, T_ID, TT_ID, L1, L2, ROWTIME) VALUES (3, NULL, 3, NULL, 'd', 60000);
ASSERT VALUES `OUTER_JOIN` (ID, T_ID, TT_ID, L1, L2, ROWTIME) VALUES (3, 3, NULL, 'D', NULL, 60000);

--@test: joins - stream stream full outer join with out of order and custom grace period - JSON
CREATE STREAM LEFT_STREAM (id BIGINT KEY, l1 VARCHAR) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM RIGHT_STREAM (id BIGINT KEY, l2 VARCHAR) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTER_JOIN as SELECT ROWKEY as ID, t.id, tt.id, l1, l2 FROM LEFT_STREAM t full outer join RIGHT_STREAM tt WITHIN 1 minute GRACE PERIOD 1 minute on t.id = tt.id;
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (0, 'A', 0);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (0, 'a', 60000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (1, 'b', 330000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (2, 'c', 90000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (2, 'C', 90000);
INSERT INTO `RIGHT_STREAM` (ID, L2, ROWTIME) VALUES (3, 'd', 60000);
INSERT INTO `LEFT_STREAM` (ID, L1, ROWTIME) VALUES (3, 'D', 60000);
ASSERT VALUES `OUTER_JOIN` (ID, T_ID, TT_ID, L1, L2, ROWTIME) VALUES (0, 0, 0, 'A', 'a', 60000);
ASSERT VALUES `OUTER_JOIN` (ID, T_ID, TT_ID, L1, L2, ROWTIME) VALUES (2, NULL, 2, NULL, 'c', 90000);
ASSERT VALUES `OUTER_JOIN` (ID, T_ID, TT_ID, L1, L2, ROWTIME) VALUES (2, 2, 2, 'C', 'c', 90000);
ASSERT VALUES `OUTER_JOIN` (ID, T_ID, TT_ID, L1, L2, ROWTIME) VALUES (3, NULL, 3, NULL, 'd', 60000);
ASSERT VALUES `OUTER_JOIN` (ID, T_ID, TT_ID, L1, L2, ROWTIME) VALUES (3, 3, NULL, 'D', NULL, 60000);

--@test: joins - table table join with where clause
CREATE TABLE TEST (id BIGINT PRIMARY KEY, name VARCHAR, value BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE TEST_TABLE (id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE OUTPUT as SELECT t.id, name, tt.f1, f2 FROM test t JOIN test_table tt ON t.id = tt.id WHERE t.value > 10 AND tt.f2 > 5;
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `TEST_TABLE` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 10000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `TEST_TABLE` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (0, 'bar', 99, 16000);
INSERT INTO `TEST` (ID, NAME, VALUE, ROWTIME) VALUES (90, 'ninety', 90, 17000);
INSERT INTO `TEST_TABLE` (ID, F1, F2, ROWTIME) VALUES (90, 'b', 10, 18000);
INSERT INTO `TEST_TABLE` (ID, ROWTIME) VALUES (90, 19000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, F1, F2, ROWTIME) VALUES (0, 'foo', 'a', 10, 15000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, F1, F2, ROWTIME) VALUES (0, 'bar', 'a', 10, 16000);
ASSERT VALUES `OUTPUT` (T_ID, NAME, F1, F2, ROWTIME) VALUES (90, 'ninety', 'b', 10, 18000);
ASSERT VALUES `OUTPUT` (T_ID, ROWTIME) VALUES (90, 19000);
ASSERT table OUTPUT (T_ID BIGINT PRIMARY KEY, `NAME` STRING, `F1` STRING, `F2` BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream to stream wrapped single field value schema on inputs
CREATE STREAM S1 (ID BIGINT KEY, NAME STRING) WITH (kafka_topic='S1', value_format='JSON');
CREATE STREAM S2 (ID BIGINT KEY, NAME STRING) WITH (kafka_topic='S2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT s1.id, s1.name name1, s2.name name2 FROM S1 JOIN S2 WITHIN 1 second ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, ROWTIME) VALUES (0, 'a', 0);
INSERT INTO `S2` (ID, NAME, ROWTIME) VALUES (0, 'b', 10);
INSERT INTO `S1` (ID, ID, ROWTIME) VALUES (0, NULL, 20);
INSERT INTO `S2` (ID, ID, ROWTIME) VALUES (0, NULL, 30);
ASSERT VALUES `OUTPUT` (S1_ID, NAME1, NAME2, ROWTIME) VALUES (0, 'a', 'b', 10);
ASSERT VALUES `OUTPUT` (S1_ID, NAME1, NAME2, ROWTIME) VALUES (0, NULL, 'b', 20);
ASSERT VALUES `OUTPUT` (S1_ID, NAME1, NAME2, ROWTIME) VALUES (0, 'a', NULL, 30);
ASSERT VALUES `OUTPUT` (S1_ID, NAME1, NAME2, ROWTIME) VALUES (0, NULL, NULL, 30);

--@test: joins - stream to stream unwrapped single field value schema on inputs
CREATE STREAM S1 (ID BIGINT KEY, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, kafka_topic='S1', value_format='JSON');
CREATE STREAM S2 (ID BIGINT KEY, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, kafka_topic='S2', value_format='JSON');
CREATE STREAM OUTPUT as SELECT s1.id, s1.name name1, s2.name name2 FROM S1 JOIN S2 WITHIN 1 second ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, ROWTIME) VALUES (0, 'a', 0);
INSERT INTO `S2` (ID, NAME, ROWTIME) VALUES (0, 'b', 10);
INSERT INTO `S1` (ID, ROWTIME) VALUES (0, 20);
INSERT INTO `S2` (ID, ROWTIME) VALUES (0, 30);
ASSERT VALUES `OUTPUT` (S1_ID, NAME1, NAME2, ROWTIME) VALUES (0, 'a', 'b', 10);

--@test: joins - stream to stream unwrapped single field value schema on inputs and output
CREATE STREAM S1 (ID BIGINT KEY, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, kafka_topic='S1', value_format='JSON');
CREATE STREAM S2 (ID BIGINT KEY, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, kafka_topic='S2', value_format='JSON');
CREATE STREAM OUTPUT WITH (WRAP_SINGLE_VALUE=false) AS SELECT s1.id, s1.name name FROM S1 JOIN S2 WITHIN 1 second ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, ROWTIME) VALUES (0, 'a', 0);
INSERT INTO `S2` (ID, NAME, ROWTIME) VALUES (0, 'b', 10);
INSERT INTO `S1` (ID, ROWTIME) VALUES (0, 20);
INSERT INTO `S2` (ID, ROWTIME) VALUES (0, 30);
ASSERT VALUES `OUTPUT` (S1_ID, NAME, ROWTIME) VALUES (0, 'a', 10);

--@test: joins - stream to table wrapped single field value schema on inputs
CREATE STREAM S (ID BIGINT KEY, NAME STRING) WITH (kafka_topic='S', value_format='JSON');
CREATE TABLE T (ID BIGINT PRIMARY KEY, NAME STRING) WITH (kafka_topic='T', value_format='JSON');
CREATE STREAM OUTPUT as SELECT s.id, s.name name1, t.name name2 FROM S JOIN T ON S.id = T.id;
INSERT INTO `T` (ID, NAME, ROWTIME) VALUES (0, 'b', 0);
INSERT INTO `S` (ID, NAME, ROWTIME) VALUES (0, 'a', 10);
INSERT INTO `S` (ID, NAME, ROWTIME) VALUES (0, NULL, 20);
INSERT INTO `T` (ID, NAME, ROWTIME) VALUES (0, NULL, 30);
INSERT INTO `S` (ID, NAME, ROWTIME) VALUES (0, NULL, 40);
INSERT INTO `T` (ID, ROWTIME) VALUES (0, 50);
INSERT INTO `S` (ID, NAME, ROWTIME) VALUES (0, 'a', 60);
ASSERT VALUES `OUTPUT` (S_ID, NAME1, NAME2, ROWTIME) VALUES (0, 'a', 'b', 10);
ASSERT VALUES `OUTPUT` (S_ID, NAME1, NAME2, ROWTIME) VALUES (0, NULL, 'b', 20);
ASSERT VALUES `OUTPUT` (S_ID, NAME1, NAME2, ROWTIME) VALUES (0, NULL, NULL, 40);

--@test: joins - stream to table unwrapped single field value schema on inputs
CREATE STREAM S (ID BIGINT KEY, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, kafka_topic='S', value_format='JSON');
CREATE TABLE T (ID BIGINT PRIMARY KEY, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, kafka_topic='T', value_format='JSON');
CREATE STREAM OUTPUT as SELECT s.id, s.name name1, t.name name2 FROM S JOIN T ON S.id = T.id;
INSERT INTO `T` (ID, NAME, ROWTIME) VALUES (0, 'b', 0);
INSERT INTO `S` (ID, NAME, ROWTIME) VALUES (0, 'a', 10);
INSERT INTO `S` (ID, ROWTIME) VALUES (0, 20);
INSERT INTO `T` (ID, ROWTIME) VALUES (0, 30);
INSERT INTO `S` (ID, ROWTIME) VALUES (0, 40);
ASSERT VALUES `OUTPUT` (S_ID, NAME1, NAME2, ROWTIME) VALUES (0, 'a', 'b', 10);

--@test: joins - stream to table unwrapped single field value schema on inputs and output
CREATE STREAM S (ID BIGINT KEY, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, kafka_topic='S', value_format='JSON');
CREATE TABLE T (ID BIGINT PRIMARY KEY, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, kafka_topic='T', value_format='JSON');
CREATE STREAM OUTPUT WITH (WRAP_SINGLE_VALUE=false) AS SELECT s.id, s.name name FROM S JOIN T ON S.id = T.id;
INSERT INTO `T` (ID, NAME, ROWTIME) VALUES (0, 'b', 0);
INSERT INTO `S` (ID, NAME, ROWTIME) VALUES (0, 'a', 10);
INSERT INTO `S` (ID, ROWTIME) VALUES (0, 20);
INSERT INTO `T` (ID, ROWTIME) VALUES (0, 30);
INSERT INTO `S` (ID, ROWTIME) VALUES (0, 40);
ASSERT VALUES `OUTPUT` (S_ID, NAME, ROWTIME) VALUES (0, 'a', 10);

--@test: joins - table to table wrapped single field value schema on inputs
CREATE TABLE T1 (ID BIGINT PRIMARY KEY, NAME STRING) WITH (kafka_topic='T1', value_format='JSON');
CREATE TABLE T2 (ID BIGINT PRIMARY KEY, NAME STRING) WITH (kafka_topic='T2', value_format='JSON');
CREATE TABLE OUTPUT as SELECT t1.id, t1.name name1, t2.name name2 FROM T1 JOIN T2 ON T1.id = T2.id;
INSERT INTO `T1` (ID, NAME, ROWTIME) VALUES (0, 'a', 0);
INSERT INTO `T2` (ID, NAME, ROWTIME) VALUES (0, 'b', 10);
INSERT INTO `T1` (ID, NAME, ROWTIME) VALUES (0, NULL, 20);
INSERT INTO `T2` (ID, NAME, ROWTIME) VALUES (0, NULL, 30);
INSERT INTO `T1` (ID, NAME, ROWTIME) VALUES (0, NULL, 40);
INSERT INTO `T1` (ID, ROWTIME) VALUES (0, 50);
INSERT INTO `T2` (ID, ROWTIME) VALUES (0, 60);
ASSERT VALUES `OUTPUT` (T1_ID, NAME1, NAME2, ROWTIME) VALUES (0, 'a', 'b', 10);
ASSERT VALUES `OUTPUT` (T1_ID, NAME1, NAME2, ROWTIME) VALUES (0, NULL, 'b', 20);
ASSERT VALUES `OUTPUT` (T1_ID, NAME1, NAME2, ROWTIME) VALUES (0, NULL, NULL, 30);
ASSERT VALUES `OUTPUT` (T1_ID, NAME1, NAME2, ROWTIME) VALUES (0, NULL, NULL, 40);
ASSERT VALUES `OUTPUT` (T1_ID, ROWTIME) VALUES (0, 50);

--@test: joins - table to table unwrapped single field value schema on inputs
CREATE TABLE T1 (ID BIGINT PRIMARY KEY, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, kafka_topic='T1', value_format='JSON');
CREATE TABLE T2 (ID BIGINT PRIMARY KEY, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, kafka_topic='T2', value_format='JSON');
CREATE TABLE OUTPUT as SELECT t1.id, t1.name name1, t2.name name2 FROM T1 JOIN T2 ON T1.id = T2.id;
INSERT INTO `T1` (ID, NAME, ROWTIME) VALUES (0, 'a', 0);
INSERT INTO `T2` (ID, NAME, ROWTIME) VALUES (0, 'b', 10);
INSERT INTO `T1` (ID, ROWTIME) VALUES (0, 20);
INSERT INTO `T2` (ID, ROWTIME) VALUES (0, 30);
INSERT INTO `T1` (ID, ROWTIME) VALUES (0, 40);
ASSERT VALUES `OUTPUT` (T1_ID, NAME1, NAME2, ROWTIME) VALUES (0, 'a', 'b', 10);
ASSERT VALUES `OUTPUT` (T1_ID, ROWTIME) VALUES (0, 20);

--@test: joins - table to table unwrapped single field value schema on inputs and output
CREATE TABLE T1 (ID BIGINT PRIMARY KEY, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, kafka_topic='T1', value_format='JSON');
CREATE TABLE T2 (ID BIGINT PRIMARY KEY, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, kafka_topic='T2', value_format='JSON');
CREATE TABLE OUTPUT WITH (WRAP_SINGLE_VALUE=false) AS SELECT t1.id, t1.name name FROM T1 JOIN T2 ON T1.id = T2.id;
INSERT INTO `T1` (ID, NAME, ROWTIME) VALUES (0, 'a', 0);
INSERT INTO `T2` (ID, NAME, ROWTIME) VALUES (0, 'b', 10);
INSERT INTO `T1` (ID, ROWTIME) VALUES (0, 20);
INSERT INTO `T2` (ID, ROWTIME) VALUES (0, 30);
INSERT INTO `T1` (ID, ROWTIME) VALUES (0, 40);
ASSERT VALUES `OUTPUT` (T1_ID, NAME, ROWTIME) VALUES (0, 'a', 10);
ASSERT VALUES `OUTPUT` (T1_ID, ROWTIME) VALUES (0, 20);

--@test: joins - stream stream left join - invalid join field - contains literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid comparison expression '0' in join '(T.ID = 0)'. Each side of the join comparision must contain references from exactly one source.
CREATE STREAM TEST1 (ID BIGINT KEY, NAME STRING) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (ID BIGINT KEY, NAME STRING) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM test1 t left join test2 tt ON t.id = 0;
--@test: joins - stream stream left join - invalid join field on lhs- contains literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid comparison expression '0' in join '(0 = T.ID)'. Each side of the join comparision must contain references from exactly one source.
CREATE STREAM TEST1 (ID BIGINT KEY, NAME STRING) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (ID BIGINT KEY, NAME STRING) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM test1 t left join test2 tt ON 0 = t.id;
--@test: joins - stream stream right join - invalid join field - contains literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid comparison expression '0' in join '(T.ID = 0)'. Each side of the join comparision must contain references from exactly one source.
CREATE STREAM TEST1 (ID BIGINT KEY, NAME STRING) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (ID BIGINT KEY, NAME STRING) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM test1 t RIGHT JOIN test2 tt ON t.id = 0;
--@test: joins - stream stream right join - invalid join field on lhs- contains literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid comparison expression '0' in join '(0 = T.ID)'. Each side of the join comparision must contain references from exactly one source.
CREATE STREAM TEST1 (ID BIGINT KEY, NAME STRING) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (ID BIGINT KEY, NAME STRING) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM test1 t RIGHT JOIN test2 tt ON 0 = t.id;
--@test: joins - stream stream join - contains function
CREATE STREAM TEST1 (K STRING KEY, ID varchar) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (K STRING KEY, ID varchar) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT T.ID, tt.ID FROM test1 t join test2 tt WITHIN 30 SECONDS ON t.id = SUBSTRING(tt.id, 2);
INSERT INTO `TEST1` (K, id, ROWTIME) VALUES ('foo', 'foo', 0);
INSERT INTO `TEST2` (K, id, ROWTIME) VALUES ('!foo', '!foo', 10);
ASSERT VALUES `OUTPUT` (T_ID, TT_ID, ROWTIME) VALUES ('foo', '!foo', 10);

--@test: joins - stream stream join - contains CAST
CREATE STREAM TEST1 (ID bigint KEY, x bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (ID int KEY, x int) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT t.id, t.x FROM test1 t JOIN test2 tt WITHIN 30 seconds ON t.id = CAST(tt.id AS BIGINT);
INSERT INTO `TEST1` (ID, x, ROWTIME) VALUES (1, 2, 10);
INSERT INTO `TEST2` (ID, x, ROWTIME) VALUES (1, 3, 10);
ASSERT VALUES `OUTPUT` (T_ID, T_X, ROWTIME) VALUES (1, 2, 10);

--@test: joins - stream stream join - contains CAST double to int
CREATE STREAM L (ID INT KEY, x bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM R (ID DOUBLE KEY, x bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT l.id, L.x FROM L JOIN R WITHIN 30 seconds ON L.id = CAST(R.id AS INT);
INSERT INTO `L` (ID, x, ROWTIME) VALUES (1, 2, 10);
INSERT INTO `R` (ID, x, ROWTIME) VALUES (1.0, 3, 11);
ASSERT VALUES `OUTPUT` (L_ID, L_X, ROWTIME) VALUES (1, 2, 11);

--@test: joins - stream stream join on expression where schema contains ROWKEY_xx column names
CREATE STREAM TEST1 (ROWKEY bigint KEY, ROWKEY_2 bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (ROWKEY_1 int KEY, ROWKEY_3 int) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT t.ROWKEY, t.ROWKEY_2 FROM test1 t JOIN test2 tt WITHIN 30 seconds ON t.ROWKEY = CAST(tt.ROWKEY_1 AS BIGINT);
INSERT INTO `TEST1` (ROWKEY, ROWKEY_2, ROWTIME) VALUES (1, 2, 10);
INSERT INTO `TEST2` (ROWKEY_1, ROWKEY_3, ROWTIME) VALUES (1, 3, 10);
ASSERT VALUES `OUTPUT` (ROWKEY, ROWKEY_2, ROWTIME) VALUES (1, 2, 10);

--@test: joins - with generated column name clashes
CREATE TABLE L (ROWKEY INT PRIMARY KEY, ROWKEY_1 INT, ROWKEY_2 INT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE R (ROWKEY_3 INT PRIMARY KEY, ROWKEY_4 INT, ROWKEY_5 INT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE OUTPUT as SELECT ROWKEY_6, L.ROWKEY, R.ROWKEY_3, L.ROWKEY_1, R.ROWKEY_5 FROM L FULL OUTER JOIN R on L.ROWKEY = R.ROWKEY_3;
INSERT INTO `L` (ROWKEY, ROWKEY_1, ROWKEY_2, ROWTIME) VALUES (1, 2, 3, 0);
INSERT INTO `R` (ROWKEY_3, ROWKEY_4, ROWKEY_5, ROWTIME) VALUES (1, 4, 5, 100);
ASSERT VALUES `OUTPUT` (ROWKEY_6, ROWKEY, ROWKEY_3, ROWKEY_1, ROWKEY_5, ROWTIME) VALUES (1, 1, NULL, 2, NULL, 0);
ASSERT VALUES `OUTPUT` (ROWKEY_6, ROWKEY, ROWKEY_3, ROWKEY_1, ROWKEY_5, ROWTIME) VALUES (1, 1, 1, 2, 5, 100);
ASSERT table OUTPUT (ROWKEY_6 INT PRIMARY KEY, ROWKEY INT, ROWKEY_3 INT, ROWKEY_1 INT, ROWKEY_5 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - stream stream join - contains subscript
CREATE STREAM TEST1 (ID INT KEY, NAME STRING) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (K STRING KEY, ID ARRAY<INT>) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT T.ID, TT.K FROM test1 t JOIN test2 tt WITHIN 30 SECONDS ON t.id = tt.id[1];
INSERT INTO `TEST1` (ID, name, ROWTIME) VALUES (1, '-', 0);
INSERT INTO `TEST1` (ID, name, ROWTIME) VALUES (2, '-', 5);
INSERT INTO `TEST2` (K, id, ROWTIME) VALUES ('k', ARRAY[1, 2, 3], 10);
ASSERT VALUES `OUTPUT` (T_ID, K, ROWTIME) VALUES (1, 'k', 10);

--@test: joins - stream stream join - contains arithmetic binary expression
CREATE STREAM TEST1 (ID INT KEY, NAME STRING) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (ID INT KEY, NAME STRING) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT T.ID, TT.ID FROM test1 t join test2 tt WITHIN 30 seconds ON t.id = tt.id + 1;
INSERT INTO `TEST1` (ID, name, ROWTIME) VALUES (1, '-', 0);
INSERT INTO `TEST2` (ID, name, ROWTIME) VALUES (0, '-', 10);
ASSERT VALUES `OUTPUT` (T_ID, TT_ID, ROWTIME) VALUES (1, 0, 10);

--@test: joins - stream stream join - contains arithmetic unary expression
CREATE STREAM TEST1 (ID INT KEY, NAME STRING) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (ID INT KEY, NAME STRING) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT T.ID, T.NAME, TT.NAME FROM test1 t join test2 tt WITHIN 30 seconds ON t.id = -tt.id;
INSERT INTO `TEST1` (ID, name, ROWTIME) VALUES (1, 'a', 0);
INSERT INTO `TEST2` (ID, name, ROWTIME) VALUES (-1, 'b', 10);
ASSERT VALUES `OUTPUT` (T_ID, T_NAME, TT_NAME, ROWTIME) VALUES (1, 'a', 'b', 10);

--@test: joins - stream stream join - contains CASE expression
CREATE STREAM TEST1 (ID INT KEY, IGNORED STRING) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (ID INT KEY, IGNORED STRING) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT T.ID, TT.ID FROM test1 t join test2 tt WITHIN 30 SECONDS ON t.id = (CASE WHEN tt.id = 2 THEN 1 ELSE 3 END);
INSERT INTO `TEST1` (ID, ROWTIME) VALUES (1, 0);
INSERT INTO `TEST1` (ID, ROWTIME) VALUES (3, 5);
INSERT INTO `TEST2` (ID, ROWTIME) VALUES (2, 10);
ASSERT VALUES `OUTPUT` (T_ID, TT_ID, ROWTIME) VALUES (1, 2, 10);

--@test: joins - stream stream join - contains arithmetic unary expression flipped sides
CREATE STREAM TEST1 (ID INT KEY, IGNORED STRING) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (ID INT KEY, IGNORED STRING) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT T.ID, TT.ID FROM test1 t join test2 tt WITHIN 30 seconds ON -tt.id = t.id;
INSERT INTO `TEST1` (ID, ROWTIME) VALUES (1, 0);
INSERT INTO `TEST2` (ID, ROWTIME) VALUES (-1, 10);
ASSERT VALUES `OUTPUT` (T_ID, TT_ID, ROWTIME) VALUES (1, -1, 10);

--@test: joins - stream stream left join - invalid left join expression - field does not exist
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: JOIN ON column 'T.IID' cannot be resolved.
CREATE STREAM TEST1 (ID INT KEY, IGNORED STRING) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (ID INT KEY, IGNORED STRING) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM test1 t left join test2 tt ON t.iid= tt.id;
--@test: joins - stream stream left join - invalid right join expression - field does not exist
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: JOIN ON column 'TT.IID' cannot be resolved.
CREATE STREAM TEST1 (ID INT KEY, IGNORED STRING) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (ID INT KEY, IGNORED STRING) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM test1 t left join test2 tt ON t.id= tt.iid;
--@test: joins - stream stream right join - invalid left join expression - field does not exist
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Line: 3, Col: 70: JOIN ON column 'T.IID' cannot be resolved.
CREATE STREAM TEST1 (ID INT KEY, IGNORED STRING) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (ID INT KEY, IGNORED STRING) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM test1 t RIGHT JOIN test2 tt ON t.iid = tt.id;
--@test: joins - stream stream right join - invalid right join expression - field does not exist
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Line: 3, Col: 77: JOIN ON column 'TT.IID' cannot be resolved.
CREATE STREAM TEST1 (ID INT KEY, IGNORED STRING) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST2 (ID INT KEY, IGNORED STRING) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM test1 t RIGHT JOIN test2 tt ON t.id = tt.iid;
--@test: joins - Should fail on ambiguous join attribute
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Exception while preparing statement: Line: 3, Col: 84: Column 'ID' is ambiguous. Could be LEFT_TABLE.ID or RIGHT_TABLE.ID.
CREATE TABLE left_table (id BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (id BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, id2, f1, f2 FROM left_table JOIN right_table ON id = id;
--@test: joins - Should fail on unknown qualifier -- left join attribute
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Failed to prepare statement: 'UNKNOWN' is not a valid stream/table name or alias.
CREATE TABLE left_table (id BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (id BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, id2, f1, f2 FROM left_table JOIN right_table ON unknown.id = id;
--@test: joins - Should fail on unknown qualifier -- right join attribute
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Failed to prepare statement: 'UNKNOWN' is not a valid stream/table name or alias.
CREATE TABLE left_table (id BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (id BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, id2, f1, f2 FROM left_table JOIN right_table ON id = unknown.id;
--@test: joins - should not allow complex join conditions
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: joins on multiple conditions are not yet supported. Got ((L.A = R.A) AND (L.A = R.A)).
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM L INNER JOIN R WITHIN 10 SECONDS ON L.A = R.A AND L.A = R.A;
--@test: joins - unqualified join criteria
CREATE STREAM TEST (LEFT_ID BIGINT KEY, NAME varchar) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM TEST_STREAM (RIGHT_ID BIGINT KEY, F1 varchar) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT left_id, name, f1 FROM test t left join TEST_STREAM tt WITHIN 11 seconds ON left_id = right_id;
INSERT INTO `TEST` (LEFT_ID, NAME, ROWTIME) VALUES (0, 'zero', 0);
INSERT INTO `TEST_STREAM` (RIGHT_ID, F1, ROWTIME) VALUES (0, 'blah', 10000);
INSERT INTO `TEST` (LEFT_ID, NAME, ROWTIME) VALUES (10, '100', 11000);
INSERT INTO `TEST` (LEFT_ID, NAME, ROWTIME) VALUES (0, 'foo', 13000);
INSERT INTO `TEST_STREAM` (RIGHT_ID, F1, ROWTIME) VALUES (0, 'a', 15000);
INSERT INTO `TEST_STREAM` (RIGHT_ID, F1, ROWTIME) VALUES (100, 'newblah', 16000);
INSERT INTO `TEST` (LEFT_ID, NAME, ROWTIME) VALUES (90, 'ninety', 17000);
INSERT INTO `TEST` (LEFT_ID, NAME, ROWTIME) VALUES (0, 'bar', 30000);
ASSERT VALUES `OUTPUT` (LEFT_ID, NAME, F1, ROWTIME) VALUES (0, 'zero', NULL, 0);
ASSERT VALUES `OUTPUT` (LEFT_ID, NAME, F1, ROWTIME) VALUES (0, 'zero', 'blah', 10000);
ASSERT VALUES `OUTPUT` (LEFT_ID, NAME, F1, ROWTIME) VALUES (10, '100', NULL, 11000);
ASSERT VALUES `OUTPUT` (LEFT_ID, NAME, F1, ROWTIME) VALUES (0, 'foo', 'blah', 13000);
ASSERT VALUES `OUTPUT` (LEFT_ID, NAME, F1, ROWTIME) VALUES (0, 'foo', 'a', 15000);
ASSERT VALUES `OUTPUT` (LEFT_ID, NAME, F1, ROWTIME) VALUES (90, 'ninety', NULL, 17000);
ASSERT VALUES `OUTPUT` (LEFT_ID, NAME, F1, ROWTIME) VALUES (0, 'bar', NULL, 30000);
ASSERT stream OUTPUT (LEFT_ID BIGINT KEY, NAME STRING, F1 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on non-STRING value column
CREATE STREAM INPUT_STREAM (K STRING KEY, SF BIGINT) WITH (kafka_topic='stream_topic', value_format='JSON');
CREATE TABLE INPUT_TABLE (ID BIGINT PRIMARY KEY, TF INT) WITH (kafka_topic='table_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT *, S.ROWTIME, T.ROWTIME FROM INPUT_STREAM S JOIN INPUT_TABLE T on S.SF = T.ID;
INSERT INTO `INPUT_TABLE` (ID, TF, ROWTIME) VALUES (26589, 1, 0);
INSERT INTO `INPUT_STREAM` (K, SF, ROWTIME) VALUES ('a', 12589, 100);
INSERT INTO `INPUT_TABLE` (ID, TF, ROWTIME) VALUES (12589, 12, 200);
INSERT INTO `INPUT_STREAM` (K, SF, ROWTIME) VALUES ('b', 12589, 300);
ASSERT VALUES `OUTPUT` (S_SF, S_K, S_ROWTIME, T_ROWTIME, T_ID, T_TF, ROWTIME) VALUES (12589, 'b', 300, 200, 12589, 12, 300);
ASSERT stream OUTPUT (S_SF BIGINT KEY, S_K STRING, T_ID BIGINT, T_TF INT, S_ROWTIME BIGINT, T_ROWTIME BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on INT column - KAFKA - AVRO
CREATE STREAM L (ID STRING KEY, l0 INT, l1 INT) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM R (ID STRING KEY, r0 INT, r1 INT) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 10, 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 10, 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (10, 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 INT KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on INT column - KAFKA - JSON
CREATE STREAM L (ID STRING KEY, l0 INT, l1 INT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM R (ID STRING KEY, r0 INT, r1 INT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 10, 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 10, 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (10, 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 INT KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on INT column - KAFKA - PROTOBUF
CREATE STREAM L (ID STRING KEY, l0 INT, l1 INT) WITH (kafka_topic='left_topic', value_format='PROTOBUF');
CREATE STREAM R (ID STRING KEY, r0 INT, r1 INT) WITH (kafka_topic='right_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 10, 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 10, 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (10, 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 INT KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on INT column - KAFKA - PROTOBUF_NOSR
CREATE STREAM L (ID STRING KEY, l0 INT, l1 INT) WITH (kafka_topic='left_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM R (ID STRING KEY, r0 INT, r1 INT) WITH (kafka_topic='right_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 10, 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 10, 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (10, 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 INT KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on BIGINT column - KAFKA - AVRO
CREATE STREAM L (ID STRING KEY, l0 BIGINT, l1 INT) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM R (ID STRING KEY, r0 BIGINT, r1 INT) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 1000000000, 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 1000000000, 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (1000000000, 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 BIGINT KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on BIGINT column - KAFKA - JSON
CREATE STREAM L (ID STRING KEY, l0 BIGINT, l1 INT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM R (ID STRING KEY, r0 BIGINT, r1 INT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 1000000000, 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 1000000000, 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (1000000000, 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 BIGINT KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on BIGINT column - KAFKA - PROTOBUF
CREATE STREAM L (ID STRING KEY, l0 BIGINT, l1 INT) WITH (kafka_topic='left_topic', value_format='PROTOBUF');
CREATE STREAM R (ID STRING KEY, r0 BIGINT, r1 INT) WITH (kafka_topic='right_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 1000000000, 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 1000000000, 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (1000000000, 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 BIGINT KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on BIGINT column - KAFKA - PROTOBUF_NOSR
CREATE STREAM L (ID STRING KEY, l0 BIGINT, l1 INT) WITH (kafka_topic='left_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM R (ID STRING KEY, r0 BIGINT, r1 INT) WITH (kafka_topic='right_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 1000000000, 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 1000000000, 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (1000000000, 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 BIGINT KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on DOUBLE column = KAFKA - AVRO
CREATE STREAM L (ID STRING KEY, l0 DOUBLE, l1 INT) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM R (ID STRING KEY, r0 DOUBLE, r1 INT) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 1.23, 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 1.23, 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (1.23, 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 DOUBLE KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on DOUBLE column = KAFKA - JSON
CREATE STREAM L (ID STRING KEY, l0 DOUBLE, l1 INT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM R (ID STRING KEY, r0 DOUBLE, r1 INT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 1.23, 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 1.23, 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (1.23, 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 DOUBLE KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on DOUBLE column = KAFKA - PROTOBUF
CREATE STREAM L (ID STRING KEY, l0 DOUBLE, l1 INT) WITH (kafka_topic='left_topic', value_format='PROTOBUF');
CREATE STREAM R (ID STRING KEY, r0 DOUBLE, r1 INT) WITH (kafka_topic='right_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 1.23, 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 1.23, 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (1.23, 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 DOUBLE KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on DOUBLE column = KAFKA - PROTOBUF_NOSR
CREATE STREAM L (ID STRING KEY, l0 DOUBLE, l1 INT) WITH (kafka_topic='left_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM R (ID STRING KEY, r0 DOUBLE, r1 INT) WITH (kafka_topic='right_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 1.23, 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 1.23, 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (1.23, 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 DOUBLE KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on STRING column - KAFKA - AVRO
CREATE STREAM L (ID STRING KEY, l0 STRING, l1 INT) WITH (kafka_topic='left_topic', value_format='AVRO');
CREATE STREAM R (ID STRING KEY, r0 STRING, r1 INT) WITH (kafka_topic='right_topic', value_format='AVRO');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 'x', 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 'x', 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES ('x', 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 STRING KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on STRING column - KAFKA - JSON
CREATE STREAM L (ID STRING KEY, l0 STRING, l1 INT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM R (ID STRING KEY, r0 STRING, r1 INT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 'x', 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 'x', 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES ('x', 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 STRING KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on STRING column - KAFKA - PROTOBUF
CREATE STREAM L (ID STRING KEY, l0 STRING, l1 INT) WITH (kafka_topic='left_topic', value_format='PROTOBUF');
CREATE STREAM R (ID STRING KEY, r0 STRING, r1 INT) WITH (kafka_topic='right_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 'x', 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 'x', 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES ('x', 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 STRING KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on STRING column - KAFKA - PROTOBUF_NOSR
CREATE STREAM L (ID STRING KEY, l0 STRING, l1 INT) WITH (kafka_topic='left_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM R (ID STRING KEY, r0 STRING, r1 INT) WITH (kafka_topic='right_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', 'x', 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', 'x', 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES ('x', 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 STRING KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on ARRAY column - AVRO
CREATE STREAM L (ID STRING KEY, l0 ARRAY<INT>, l1 INT) WITH (kafka_topic='left_topic', format='AVRO');
CREATE STREAM R (ID STRING KEY, r0 ARRAY<INT>, r1 INT) WITH (kafka_topic='right_topic', format='AVRO');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', ARRAY[3, 1], 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', ARRAY[3, 1], 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (ARRAY[3, 1], 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 ARRAY<INT> KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on ARRAY column - JSON
CREATE STREAM L (ID STRING KEY, l0 ARRAY<INT>, l1 INT) WITH (kafka_topic='left_topic', format='JSON');
CREATE STREAM R (ID STRING KEY, r0 ARRAY<INT>, r1 INT) WITH (kafka_topic='right_topic', format='JSON');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', ARRAY[3, 1], 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', ARRAY[3, 1], 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (ARRAY[3, 1], 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 ARRAY<INT> KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on STRUCT column - AVRO
CREATE STREAM L (ID STRING KEY, l0 STRUCT<F1 INT, F2 STRING>, l1 INT) WITH (kafka_topic='left_topic', format='AVRO');
CREATE STREAM R (ID STRING KEY, r0 STRUCT<F1 INT, F2 STRING>, r1 INT) WITH (kafka_topic='right_topic', format='AVRO');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', STRUCT(F1:=2, F2:='foo'), 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', STRUCT(F1:=2, F2:='foo'), 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (STRUCT(F1:=2, F2:='foo'), 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 STRUCT<F1 INT, F2 STRING> KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on STRUCT column - JSON
CREATE STREAM L (ID STRING KEY, l0 STRUCT<F1 INT, F2 STRING>, l1 INT) WITH (kafka_topic='left_topic', format='JSON');
CREATE STREAM R (ID STRING KEY, r0 STRUCT<F1 INT, F2 STRING>, r1 INT) WITH (kafka_topic='right_topic', format='JSON');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
INSERT INTO `L` (ID, L0, L1, ROWTIME) VALUES ('a', STRUCT(F1:=2, F2:='foo'), 1, 0);
INSERT INTO `R` (ID, R0, R1, ROWTIME) VALUES ('b', STRUCT(F1:=2, F2:='foo'), 2, 10000);
ASSERT VALUES `OUTPUT` (L0, L_ID, L1, R1, ROWTIME) VALUES (STRUCT(F1:=2, F2:='foo'), 'a', 1, 2, 10000);
ASSERT stream OUTPUT (L0 STRUCT<F1 INT, F2 STRING> KEY, L_ID STRING, L1 INT, R1 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - on MAP column
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Map keys, including types that contain maps, are not supported as they may lead to unexpected behavior due to inconsistent serialization. Key column name: `L_L0`. Column type: MAP<STRING, INTEGER>. See https://github.com/confluentinc/ksql/issues/6621 for more.
CREATE STREAM L (ID STRING KEY, l0 MAP<STRING, INT>, l1 INT) WITH (kafka_topic='left_topic', format='JSON');
CREATE STREAM R (ID STRING KEY, r0 MAP<STRING, INT>, r1 INT) WITH (kafka_topic='right_topic', format='JSON');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
--@test: joins - on nested MAP column
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Map keys, including types that contain maps, are not supported as they may lead to unexpected behavior due to inconsistent serialization. Key column name: `L_L0`. Column type: STRUCT<`F1` MAP<STRING, INTEGER>>. See https://github.com/confluentinc/ksql/issues/6621 for more.
CREATE STREAM L (ID STRING KEY, l0 STRUCT<F1 MAP<STRING, INT>>, l1 INT) WITH (kafka_topic='left_topic', format='JSON');
CREATE STREAM R (ID STRING KEY, r0 STRUCT<F1 MAP<STRING, INT>>, r1 INT) WITH (kafka_topic='right_topic', format='JSON');
CREATE STREAM OUTPUT as SELECT L.l0, L.ID, L1, R1 FROM L join R WITHIN 11 SECONDS ON L.l0 = R.r0;
--@test: joins - self join
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Can not join 'INPUT' to 'INPUT': self joins are not yet supported.
CREATE STREAM INPUT (K STRING KEY, ID bigint) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM INPUT s1 JOIN INPUT s2 WITHIN 1 HOUR ON s1.id = s2.id;
--@test: joins - matching session-windowed
CREATE STREAM S1 (ID INT KEY, V bigint) WITH (kafka_topic='left_topic', value_format='JSON', WINDOW_TYPE='SESSION');
CREATE STREAM S2 (ID INT KEY, V bigint) WITH (kafka_topic='right_topic', value_format='JSON', WINDOW_TYPE='SESSION');
CREATE STREAM OUTPUT as SELECT S1.ID, S1.V, S2.V FROM S1 JOIN S2 WITHIN 1 MINUTE ON S1.ID = S2.ID;
INSERT INTO `S1` (ID, V, ROWTIME, WINDOWSTART, WINDOWEND) VALUES (1, 1, 765, 234, 765);
INSERT INTO `S2` (ID, V, ROWTIME, WINDOWSTART, WINDOWEND) VALUES (1, 2, 567, 234, 567);
INSERT INTO `S2` (ID, V, ROWTIME, WINDOWSTART, WINDOWEND) VALUES (1, 3, 765, 234, 765);
ASSERT VALUES `OUTPUT` (S1_ID, S1_V, S2_V, ROWTIME, WINDOWSTART, WINDOWEND) VALUES (1, 1, 3, 765, 234, 765);
ASSERT stream OUTPUT (S1_ID INT KEY, S1_V BIGINT, S2_V BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - matching time-windowed join with different windows fails
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Implicit repartitioning of windowed sources is not supported.
CREATE STREAM S1 (ID INT KEY, V bigint) WITH (kafka_topic='left_topic', value_format='JSON', WINDOW_TYPE='Hopping', WINDOW_SIZE='5 SECONDS');
CREATE STREAM S2 (ID INT KEY, V bigint) WITH (kafka_topic='right_topic', value_format='JSON', WINDOW_TYPE='Tumbling', WINDOW_SIZE='2 SECOND');
CREATE STREAM OUTPUT as SELECT *, S1.ROWTIME, S2.ROWTIME FROM S1 JOIN S2 WITHIN 1 MINUTE ON S1.ID = S2.ID;
--@test: joins - session - timed windowed
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Incompatible windowed sources.
CREATE STREAM S1 (ID INT KEY, V bigint) WITH (kafka_topic='left_topic', value_format='JSON', WINDOW_TYPE='Session');
CREATE STREAM S2 (ID INT KEY, V bigint) WITH (kafka_topic='right_topic', value_format='JSON', WINDOW_TYPE='TUMBLING', WINDOW_SIZE='1 SECOND');
CREATE STREAM OUTPUT as SELECT * FROM S1 JOIN S2 WITHIN 1 MINUTE ON S1.ID = S2.ID;
--@test: joins - windowed - non-windowed - INT
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Can not join windowed source to non-windowed source.
CREATE STREAM S1 (ID INT KEY, V bigint) WITH (kafka_topic='left_topic', value_format='JSON', WINDOW_TYPE='SESSION');
CREATE STREAM S2 (ID INT KEY, V bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM S1 JOIN S2 WITHIN 1 MINUTE ON S1.ID = S2.ID;
--@test: joins - windowed - non-windowed - STRING
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Can not join windowed source to non-windowed source.
CREATE STREAM S1 (ID STRING KEY, V bigint) WITH (kafka_topic='left_topic', value_format='JSON', WINDOW_TYPE='SESSION');
CREATE STREAM S2 (ID STRING KEY, V bigint) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM S1 JOIN S2 WITHIN 1 MINUTE ON S1.ID = S2.ID;
--@test: joins - join requiring repartition of windowed source
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Implicit repartitioning of windowed sources is not supported. See https://github.com/confluentinc/ksql/issues/4385.
CREATE STREAM S1 (K INT KEY, ID INT, V bigint) WITH (kafka_topic='left_topic', value_format='JSON', WINDOW_TYPE='SESSION');
CREATE STREAM S2 (K INT KEY, ID INT, V bigint) WITH (kafka_topic='right_topic', value_format='JSON', WINDOW_TYPE='SESSION');
CREATE STREAM OUTPUT as SELECT * FROM S1 JOIN S2 WITHIN 1 MINUTE ON S1.ID = S2.ID;
--@test: joins - on struct field
CREATE STREAM S1 (ID INT KEY, A bigint, B STRUCT<C INT>) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE STREAM S2 (ID INT KEY, X bigint, Y STRUCT<Z INT>) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT *, S1.ROWTIME, S2.ROWTIME FROM S1 JOIN S2 WITHIN 1 MINUTE ON S1.B->C = S2.Y->Z;
INSERT INTO `S1` (ID, A, B, ROWTIME) VALUES (1, 1, STRUCT(C:=10), 20);
INSERT INTO `S2` (ID, X, Y, ROWTIME) VALUES (2, 4, STRUCT(Z:=10), 100);
ASSERT VALUES `OUTPUT` (ROWKEY, S1_ROWTIME, S1_ID, S1_A, S1_B, S2_ROWTIME, S2_ID, S2_X, S2_Y, ROWTIME) VALUES (10, 20, 1, 1, STRUCT(C:=10), 100, 2, 4, STRUCT(Z:=10), 100);
ASSERT stream OUTPUT (ROWKEY INT KEY, S1_ID INT, S1_A BIGINT, S1_B STRUCT<C INT>, S2_ID INT, S2_X BIGINT, S2_Y STRUCT<Z INT>, S1_ROWTIME BIGINT, S2_ROWTIME BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - with where
CREATE STREAM impressions (user VARCHAR KEY, impression_id BIGINT, url VARCHAR) WITH (kafka_topic='impressions', value_format='JSON');
CREATE STREAM clicks (user VARCHAR KEY, url VARCHAR) WITH (kafka_topic='clicks', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT I.USER, IMPRESSION_ID, I.URL AS URL FROM impressions i JOIN clicks c WITHIN 1 minute ON i.user = c.user WHERE i.url = c.url;
INSERT INTO `IMPRESSIONS` (USER, impression_id, url, ROWTIME) VALUES ('user_0', 24, 'urlA', 10);
INSERT INTO `CLICKS` (USER, url, ROWTIME) VALUES ('user_0', 'urlX', 11);
INSERT INTO `CLICKS` (USER, url, ROWTIME) VALUES ('user_0', 'urlA', 12);
ASSERT VALUES `OUTPUT` (I_USER, IMPRESSION_ID, URL, ROWTIME) VALUES ('user_0', 24, 'urlA', 12);

--@test: joins - streams with no key columns (stream->stream)
CREATE STREAM L (A INT, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (A INT, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM L INNER JOIN R WITHIN 10 SECONDS ON L.A = R.A;
INSERT INTO `L` (A, B, C, ROWTIME) VALUES (0, 1, 2, 10);
INSERT INTO `R` (A, B, C, ROWTIME) VALUES (0, -1, -2, 11);
ASSERT VALUES `OUTPUT` (L_A, R_A, L_B, R_B, L_C, R_C, ROWTIME) VALUES (0, 0, 1, -1, 2, -2, 11);
ASSERT stream OUTPUT (L_A INT KEY, L_B INT, L_C INT, R_A INT, R_B INT, R_C INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - streams with no key columns (stream->table)
CREATE STREAM L (A INT, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE TABLE R (A INT PRIMARY KEY, B INT, C INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM L INNER JOIN R ON L.A = R.A;
INSERT INTO `R` (A, B, C, ROWTIME) VALUES (0, -1, -2, 10);
INSERT INTO `L` (A, B, C, ROWTIME) VALUES (0, 1, 2, 11);
ASSERT VALUES `OUTPUT` (L_A, R_A, L_B, R_B, L_C, R_C, ROWTIME) VALUES (0, 0, 1, -1, 2, -2, 11);
ASSERT stream OUTPUT (L_A INT KEY, L_B INT, L_C INT, R_A INT, R_B INT, R_C INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: joins - non-KAFKA key format
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', format='JSON');
CREATE TABLE R (A INT PRIMARY KEY, B INT, C INT) WITH (kafka_topic='RIGHT', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM L INNER JOIN R ON L.A = R.A;
INSERT INTO `R` (A, B, C, ROWTIME) VALUES (1, -1, -2, 10);
INSERT INTO `L` (A, B, C, ROWTIME) VALUES (1, 1, 2, 11);
INSERT INTO `L` (A, B, C, ROWTIME) VALUES (2, 2, 3, 12);
ASSERT VALUES `OUTPUT` (L_A, R_A, L_B, R_B, L_C, R_C, ROWTIME) VALUES (1, 1, 1, -1, 2, -2, 11);

--@test: joins - table-table key-to-key - SR-enabled key format - incompatible schemas
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: types don't match. Got T1.ID{INTEGER} = T2.ID{BIGINT}.
CREATE TABLE T1 (ID INT PRIMARY KEY, VAL STRING) WITH (kafka_topic='t1', key_format='AVRO', value_format='JSON');
CREATE TABLE T2 (ID BIGINT PRIMARY KEY, FOO INT) WITH (kafka_topic='t2', key_format='AVRO', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT T2.ID, T1.VAL FROM T1 JOIN T2 ON T1.ID = T2.ID;
--@test: joins - matching time-windowed - SR-enabled key format - fails if join on non-key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Implicit repartitioning of windowed sources is not supported. See https://github.com/confluentinc/ksql/issues/4385.
CREATE STREAM S1 (ID INT KEY, V int) WITH (kafka_topic='left_topic', key_format='JSON_SR', value_format='JSON', WINDOW_TYPE='Hopping', WINDOW_SIZE='5 SECONDS');
CREATE STREAM S2 (ID INT KEY, V int) WITH (kafka_topic='right_topic', key_format='JSON_SR', value_format='JSON', WINDOW_TYPE='Tumbling', WINDOW_SIZE='2 SECOND');
CREATE STREAM OUTPUT as SELECT *, S1.ROWTIME, S2.ROWTIME FROM S1 JOIN S2 WITHIN 1 MINUTE ON S1.ID = S2.V;
