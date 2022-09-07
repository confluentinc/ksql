--@test: fk-join - Should fail if types in join condition don't match
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: types don't match. Got LEFT_TABLE.FOREIGN_KEY_WRONG_TYPE{STRING} = RIGHT_TABLE.R_ID{BIGINT}.
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, foreign_key_wrong_type VARCHAR) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT * FROM left_table JOIN right_table ON foreign_key_wrong_type = r_id;
--@test: fk-join - Should fail if types in join condition don't match (expression)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: types don't match. Got CAST(LEFT_TABLE.FOREIGN_KEY AS STRING){STRING} = RIGHT_TABLE.R_ID{BIGINT}.
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT l_id, r_id, name, f1 FROM left_table JOIN right_table ON CAST(foreign_key AS STRING) = r_id;
--@test: fk-join - Should not allow join on partial right source key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: table-table joins require to join on the primary key of the right input table. Got LEFT_TABLE.FOREIGN_KEY = RIGHT_TABLE.R_ID_1.
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id_1 BIGINT PRIMARY KEY, r_id_2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT * FROM left_table JOIN right_table ON foreign_key = r_id_1;
--@test: fk-join - Should fail if single result key column is missing in projection for inner-join
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Primary key column missing in projection. For foreign-key table-table joins, the projection must include all primary key columns from the left input table (`LEFT_TABLE`). Missing column: `L_ID`.
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, f1 VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT f1, foreign_key, r_id, f2 FROM left_table JOIN right_table ON foreign_key = r_id;
--@test: fk-join - Should fail on partial left source key in projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Primary key column missing in projection. For foreign-key table-table joins, the projection must include all primary key columns from the left input table (`LEFT_TABLE`). Missing column: `L_ID_2`.
CREATE TABLE left_table (l_id_1 BIGINT PRIMARY KEY, l_id_2 BIGINT PRIMARY KEY, f1 VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT l_id_1, f1, foreign_key, r_id, f2 FROM left_table JOIN right_table ON foreign_key = r_id;
--@test: fk-join - Should fail if all result key columns are missing for left join
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Primary key columns missing in projection. For foreign-key table-table joins, the projection must include all primary key columns from the left input table (`LEFT_TABLE`). Missing columns: `L_ID_1`, `L_ID_2`.
CREATE TABLE left_table (l_id_1 BIGINT PRIMARY KEY, l_id_2 BIGINT PRIMARY KEY, f1 VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT f1, foreign_key, r_id, f2 FROM left_table LEFT JOIN right_table ON foreign_key = r_id;
--@test: fk-join - Should fail with duplicate left source key in projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The projection contains a key column (`LEFT_TABLE_L_ID`) more than once, aliased as: L_ID and L_ID_DUPLICATE.
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, f1 VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT l_id, l_id AS l_id_duplicate, f1, foreign_key r_id, f2 FROM left_table JOIN right_table ON foreign_key = r_id;
--@test: fk-join - Should support inner join with left value-column expression
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT l_id, r_id, name, f1 FROM left_table JOIN right_table ON foreign_key = r_id;
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 0);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 10000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (10, 'bar', 0, 16000);
INSERT INTO `RIGHT_TABLE` (R_ID, ROWTIME) VALUES (0, 17000);
INSERT INTO `LEFT_TABLE` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'blah', 10000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'a', 15000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (10, 0, 'bar', 'a', 16000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 17000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (10, 17000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT table OUTPUT (L_ID BIGINT PRIMARY KEY, R_ID BIGINT, NAME STRING, F1 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-join - Should support left join with left value-column expression
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT l_id, r_id, name, f1 FROM left_table LEFT JOIN right_table ON foreign_key = r_id;
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 0);
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 10000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 11000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (10, 'bar', 0, 16000);
INSERT INTO `RIGHT_TABLE` (R_ID, ROWTIME) VALUES (0, 17000);
INSERT INTO `LEFT_TABLE` (L_ID, ROWTIME) VALUES (1, 18000);
INSERT INTO `LEFT_TABLE` (L_ID, ROWTIME) VALUES (0, 19000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (1, NULL, 'zero', NULL, 0);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'blah', 10000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'blah', 11000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (0, NULL, 'foo', NULL, 13000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'a', 15000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (10, 0, 'bar', 'a', 16000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (1, NULL, 'zero', NULL, 17000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (10, NULL, 'bar', NULL, 17000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (0, 19000);
ASSERT table OUTPUT (L_ID BIGINT PRIMARY KEY, R_ID BIGINT, NAME STRING, F1 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-join - Should fail on a FK right join
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: RIGHT OUTER JOIN on a foreign key is not supported
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT l_id, r_id, name, f1 FROM left_table RIGHT JOIN right_table ON foreign_key = r_id;
--@test: fk-join - Should support inner join with left key-column expression
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, l_id_2_foreign_key BIGINT PRIMARY KEY, name VARCHAR, value BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT l_id, l_id_2_foreign_key, r_id, name, f1 FROM left_table JOIN right_table ON l_id_2_foreign_key = r_id;
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 0);
INSERT INTO `LEFT_TABLE` (L_ID, L_ID_2_FOREIGN_KEY, NAME, VALUE, ROWTIME) VALUES (1, 0, 'zero', 0, 10000);
INSERT INTO `LEFT_TABLE` (L_ID, L_ID_2_FOREIGN_KEY, NAME, VALUE, ROWTIME) VALUES (0, 100, 'foo', 100, 13000);
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `LEFT_TABLE` (L_ID, L_ID_2_FOREIGN_KEY, NAME, VALUE, ROWTIME) VALUES (10, 0, 'bar', 0, 16000);
INSERT INTO `RIGHT_TABLE` (R_ID, ROWTIME) VALUES (0, 17000);
INSERT INTO `LEFT_TABLE` (L_ID, L_ID_2_FOREIGN_KEY, ROWTIME) VALUES (1, 0, 18000);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 0, 'zero', 'blah', 10000);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 0, 'zero', 'a', 15000);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, R_ID, NAME, F1, ROWTIME) VALUES (10, 0, 0, 'bar', 'a', 16000);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, ROWTIME) VALUES (1, 0, 17000);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, ROWTIME) VALUES (10, 0, 17000);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, ROWTIME) VALUES (1, 0, 18000);
ASSERT table OUTPUT (L_ID BIGINT PRIMARY KEY, L_ID_2_FOREIGN_KEY BIGINT PRIMARY KEY, R_ID BIGINT, NAME STRING, F1 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-join - Should support left join with left key-column expression
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, l_id_2_foreign_key BIGINT PRIMARY KEY, name VARCHAR, value BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT l_id, l_id_2_foreign_key, r_id, name, f1 FROM left_table LEFT JOIN right_table ON l_id_2_foreign_key = r_id;
INSERT INTO `LEFT_TABLE` (L_ID, L_ID_2_FOREIGN_KEY, NAME, VALUE, ROWTIME) VALUES (1, 0, 'zero', 0, 0);
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 10000);
INSERT INTO `LEFT_TABLE` (L_ID, L_ID_2_FOREIGN_KEY, NAME, VALUE, ROWTIME) VALUES (1, 0, 'zero', 0, 11000);
INSERT INTO `LEFT_TABLE` (L_ID, L_ID_2_FOREIGN_KEY, NAME, VALUE, ROWTIME) VALUES (0, 100, 'foo', 100, 13000);
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `LEFT_TABLE` (L_ID, L_ID_2_FOREIGN_KEY, NAME, VALUE, ROWTIME) VALUES (10, 0, 'bar', 0, 16000);
INSERT INTO `RIGHT_TABLE` (R_ID, ROWTIME) VALUES (0, 17000);
INSERT INTO `LEFT_TABLE` (L_ID, L_ID_2_FOREIGN_KEY, ROWTIME) VALUES (1, 0, 18000);
INSERT INTO `LEFT_TABLE` (L_ID, L_ID_2_FOREIGN_KEY, ROWTIME) VALUES (0, 100, 19000);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, NULL, 'zero', NULL, 0);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 0, 'zero', 'blah', 10000);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 0, 'zero', 'blah', 11000);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, R_ID, NAME, F1, ROWTIME) VALUES (0, 100, NULL, 'foo', NULL, 13000);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 0, 'zero', 'a', 15000);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, R_ID, NAME, F1, ROWTIME) VALUES (10, 0, 0, 'bar', 'a', 16000);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, NULL, 'zero', NULL, 17000);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, R_ID, NAME, F1, ROWTIME) VALUES (10, 0, NULL, 'bar', NULL, 17000);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, ROWTIME) VALUES (1, 0, 18000);
ASSERT VALUES `OUTPUT` (L_ID, L_ID_2_FOREIGN_KEY, ROWTIME) VALUES (0, 100, 19000);
ASSERT table OUTPUT (L_ID BIGINT PRIMARY KEY, L_ID_2_FOREIGN_KEY BIGINT PRIMARY KEY, R_ID BIGINT, NAME STRING, F1 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-join - Should support inner join with left value-column expression - with aliases
CREATE TABLE left_table (id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT lt.id, rt.id AS rt_id_alias, name AS name_alias, rt.f1 FROM left_table AS lt JOIN right_table AS rt ON lt.foreign_key = rt.id;
INSERT INTO `RIGHT_TABLE` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 0);
INSERT INTO `LEFT_TABLE` (ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 10000);
INSERT INTO `LEFT_TABLE` (ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `RIGHT_TABLE` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `LEFT_TABLE` (ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (10, 'bar', 0, 16000);
INSERT INTO `RIGHT_TABLE` (ID, ROWTIME) VALUES (0, 17000);
INSERT INTO `LEFT_TABLE` (ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID_ALIAS, NAME_ALIAS, F1, ROWTIME) VALUES (1, 0, 'zero', 'blah', 10000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID_ALIAS, NAME_ALIAS, F1, ROWTIME) VALUES (1, 0, 'zero', 'a', 15000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID_ALIAS, NAME_ALIAS, F1, ROWTIME) VALUES (10, 0, 'bar', 'a', 16000);
ASSERT VALUES `OUTPUT` (LT_ID, ROWTIME) VALUES (1, 17000);
ASSERT VALUES `OUTPUT` (LT_ID, ROWTIME) VALUES (10, 17000);
ASSERT VALUES `OUTPUT` (LT_ID, ROWTIME) VALUES (1, 18000);
ASSERT table OUTPUT (LT_ID BIGINT PRIMARY KEY, RT_ID_ALIAS BIGINT, NAME_ALIAS STRING, F1 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-join - Should support left join with left value-column expression - with aliases
CREATE TABLE left_table (id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT lt.id, rt.id, name, rt.f1 FROM left_table AS lt LEFT JOIN right_table AS rt ON lt.foreign_key = rt.id;
INSERT INTO `LEFT_TABLE` (ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 0);
INSERT INTO `RIGHT_TABLE` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 10000);
INSERT INTO `LEFT_TABLE` (ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 11000);
INSERT INTO `LEFT_TABLE` (ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `RIGHT_TABLE` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `LEFT_TABLE` (ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (10, 'bar', 0, 16000);
INSERT INTO `RIGHT_TABLE` (ID, ROWTIME) VALUES (0, 17000);
INSERT INTO `LEFT_TABLE` (ID, ROWTIME) VALUES (1, 18000);
INSERT INTO `LEFT_TABLE` (ID, ROWTIME) VALUES (0, 19000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID, NAME, F1, ROWTIME) VALUES (1, NULL, 'zero', NULL, 0);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'blah', 10000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'blah', 11000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID, NAME, F1, ROWTIME) VALUES (0, NULL, 'foo', NULL, 13000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'a', 15000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID, NAME, F1, ROWTIME) VALUES (10, 0, 'bar', 'a', 16000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID, NAME, F1, ROWTIME) VALUES (1, NULL, 'zero', NULL, 17000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID, NAME, F1, ROWTIME) VALUES (10, NULL, 'bar', NULL, 17000);
ASSERT VALUES `OUTPUT` (LT_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (LT_ID, ROWTIME) VALUES (0, 19000);
ASSERT table OUTPUT (LT_ID BIGINT PRIMARY KEY, RT_ID BIGINT, NAME STRING, F1 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-join - Should support non-column reference in left join expression
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT l_id, r_id, name, f1 FROM left_table JOIN right_table ON foreign_key + 1 = r_id;
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 0);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', -1, 10000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (0, 'foo', 99, 13000);
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (10, 'bar', -1, 16000);
INSERT INTO `RIGHT_TABLE` (R_ID, ROWTIME) VALUES (0, 17000);
INSERT INTO `LEFT_TABLE` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'blah', 10000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'a', 15000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (10, 0, 'bar', 'a', 16000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 17000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (10, 17000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT table OUTPUT (L_ID BIGINT PRIMARY KEY, R_ID BIGINT, NAME STRING, F1 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-join - Should support non-column reference in left join expression with qualifier
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT l_id, r_id, name, f1 FROM left_table JOIN right_table ON left_table.foreign_key + 1 = r_id;
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 0);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', -1, 10000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (0, 'foo', 99, 13000);
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (10, 'bar', -1, 16000);
INSERT INTO `RIGHT_TABLE` (R_ID, ROWTIME) VALUES (0, 17000);
INSERT INTO `LEFT_TABLE` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'blah', 10000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'a', 15000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (10, 0, 'bar', 'a', 16000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 17000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (10, 17000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT table OUTPUT (L_ID BIGINT PRIMARY KEY, R_ID BIGINT, NAME STRING, F1 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-join - Should support non-column reference in left join expression with alias
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT lt.l_id, r_id, lt.name, f1 FROM left_table AS lt JOIN right_table ON lt.foreign_key + 1 = r_id;
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 0);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', -1, 10000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (0, 'foo', 99, 13000);
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (10, 'bar', -1, 16000);
INSERT INTO `RIGHT_TABLE` (R_ID, ROWTIME) VALUES (0, 17000);
INSERT INTO `LEFT_TABLE` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'blah', 10000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'a', 15000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (10, 0, 'bar', 'a', 16000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 17000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (10, 17000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT table OUTPUT (L_ID BIGINT PRIMARY KEY, R_ID BIGINT, NAME STRING, F1 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-join - Should support flipped join condition
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT l_id, r_id, name, f1 FROM left_table JOIN right_table ON r_id = foreign_key;
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 0);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 10000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (10, 'bar', 0, 16000);
INSERT INTO `RIGHT_TABLE` (R_ID, ROWTIME) VALUES (0, 17000);
INSERT INTO `LEFT_TABLE` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'blah', 10000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (1, 0, 'zero', 'a', 15000);
ASSERT VALUES `OUTPUT` (L_ID, R_ID, NAME, F1, ROWTIME) VALUES (10, 0, 'bar', 'a', 16000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 17000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (10, 17000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT table OUTPUT (L_ID BIGINT PRIMARY KEY, R_ID BIGINT, NAME STRING, F1 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-join - Should support flipped join condition with aliases
CREATE TABLE left_table (id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT lt.id, rt.id AS rt_id_alias, name AS name_alias, rt.f1 FROM left_table AS lt LEFT JOIN right_table AS rt ON rt.id = lt.foreign_key;
INSERT INTO `LEFT_TABLE` (ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 0);
INSERT INTO `RIGHT_TABLE` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 10000);
INSERT INTO `LEFT_TABLE` (ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 11000);
INSERT INTO `LEFT_TABLE` (ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `RIGHT_TABLE` (ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `LEFT_TABLE` (ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (10, 'bar', 0, 16000);
INSERT INTO `RIGHT_TABLE` (ID, ROWTIME) VALUES (0, 17000);
INSERT INTO `LEFT_TABLE` (ID, ROWTIME) VALUES (1, 18000);
INSERT INTO `LEFT_TABLE` (ID, ROWTIME) VALUES (0, 19000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID_ALIAS, NAME_ALIAS, F1, ROWTIME) VALUES (1, NULL, 'zero', NULL, 0);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID_ALIAS, NAME_ALIAS, F1, ROWTIME) VALUES (1, 0, 'zero', 'blah', 10000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID_ALIAS, NAME_ALIAS, F1, ROWTIME) VALUES (1, 0, 'zero', 'blah', 11000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID_ALIAS, NAME_ALIAS, F1, ROWTIME) VALUES (0, NULL, 'foo', NULL, 13000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID_ALIAS, NAME_ALIAS, F1, ROWTIME) VALUES (1, 0, 'zero', 'a', 15000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID_ALIAS, NAME_ALIAS, F1, ROWTIME) VALUES (10, 0, 'bar', 'a', 16000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID_ALIAS, NAME_ALIAS, F1, ROWTIME) VALUES (1, NULL, 'zero', NULL, 17000);
ASSERT VALUES `OUTPUT` (LT_ID, RT_ID_ALIAS, NAME_ALIAS, F1, ROWTIME) VALUES (10, NULL, 'bar', NULL, 17000);
ASSERT VALUES `OUTPUT` (LT_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (LT_ID, ROWTIME) VALUES (0, 19000);
ASSERT table OUTPUT (LT_ID BIGINT PRIMARY KEY, RT_ID_ALIAS BIGINT, NAME_ALIAS STRING, F1 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-join - Should allow to omit join columns in projection
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT l_id, name, f1 FROM left_table JOIN right_table ON foreign_key = r_id;
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 0);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 10000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (0, 'foo', 100, 13000);
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'a', 10, 15000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (10, 'bar', 0, 16000);
INSERT INTO `RIGHT_TABLE` (R_ID, ROWTIME) VALUES (0, 17000);
INSERT INTO `LEFT_TABLE` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (L_ID, NAME, F1, ROWTIME) VALUES (1, 'zero', 'blah', 10000);
ASSERT VALUES `OUTPUT` (L_ID, NAME, F1, ROWTIME) VALUES (1, 'zero', 'a', 15000);
ASSERT VALUES `OUTPUT` (L_ID, NAME, F1, ROWTIME) VALUES (10, 'bar', 'a', 16000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 17000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (10, 17000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT table OUTPUT (L_ID BIGINT PRIMARY KEY, NAME STRING, F1 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-join - Should support `SELECT *`
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT * FROM left_table JOIN right_table ON foreign_key = r_id;
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 0);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 10000);
INSERT INTO `RIGHT_TABLE` (R_ID, ROWTIME) VALUES (0, 17000);
ASSERT VALUES `OUTPUT` (LEFT_TABLE_L_ID, LEFT_TABLE_NAME, LEFT_TABLE_FOREIGN_KEY, RIGHT_TABLE_R_ID, RIGHT_TABLE_F1, RIGHT_TABLE_F2, ROWTIME) VALUES (1, 'zero', 0, 0, 'blah', 4, 10000);
ASSERT VALUES `OUTPUT` (LEFT_TABLE_L_ID, ROWTIME) VALUES (1, 17000);
ASSERT table OUTPUT (LEFT_TABLE_L_ID BIGINT PRIMARY KEY, LEFT_TABLE_NAME STRING, LEFT_TABLE_FOREIGN_KEY BIGINT, RIGHT_TABLE_R_ID BIGINT, RIGHT_TABLE_F1 STRING, RIGHT_TABLE_F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-join - Should support qualified `SELECT *` for right input
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT l_id, right_table.* FROM left_table LEFT JOIN right_table ON foreign_key = r_id;
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 0);
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 10000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 11000);
INSERT INTO `LEFT_TABLE` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (L_ID, RIGHT_TABLE_R_ID, RIGHT_TABLE_F1, RIGHT_TABLE_F2, ROWTIME) VALUES (1, NULL, NULL, NULL, 0);
ASSERT VALUES `OUTPUT` (L_ID, RIGHT_TABLE_R_ID, RIGHT_TABLE_F1, RIGHT_TABLE_F2, ROWTIME) VALUES (1, 0, 'blah', 4, 10000);
ASSERT VALUES `OUTPUT` (L_ID, RIGHT_TABLE_R_ID, RIGHT_TABLE_F1, RIGHT_TABLE_F2, ROWTIME) VALUES (1, 0, 'blah', 4, 11000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT table OUTPUT (L_ID BIGINT PRIMARY KEY, RIGHT_TABLE_R_ID BIGINT, RIGHT_TABLE_F1 STRING, RIGHT_TABLE_F2 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-join - Should support qualified `SELECT *` for left input
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT left_table.*, right_table.f1 FROM left_table JOIN right_table ON foreign_key = r_id;
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 0);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 10000);
INSERT INTO `RIGHT_TABLE` (R_ID, ROWTIME) VALUES (0, 17000);
ASSERT VALUES `OUTPUT` (LEFT_TABLE_L_ID, LEFT_TABLE_NAME, LEFT_TABLE_FOREIGN_KEY, F1, ROWTIME) VALUES (1, 'zero', 0, 'blah', 10000);
ASSERT VALUES `OUTPUT` (LEFT_TABLE_L_ID, ROWTIME) VALUES (1, 17000);
ASSERT table OUTPUT (LEFT_TABLE_L_ID BIGINT PRIMARY KEY, LEFT_TABLE_NAME STRING, LEFT_TABLE_FOREIGN_KEY BIGINT, F1 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-join - Should support qualified `SELECT *` for both input using aliases
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, name VARCHAR, foreign_key BIGINT) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f1 VARCHAR, f2 BIGINT) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE TABLE output AS SELECT lt.*, rt.*, lt.name AS name_alias FROM left_table AS lt LEFT JOIN right_table AS rt ON foreign_key = r_id;
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 0);
INSERT INTO `RIGHT_TABLE` (R_ID, F1, F2, ROWTIME) VALUES (0, 'blah', 4, 10000);
INSERT INTO `LEFT_TABLE` (L_ID, NAME, FOREIGN_KEY, ROWTIME) VALUES (1, 'zero', 0, 11000);
INSERT INTO `LEFT_TABLE` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (LT_L_ID, LT_NAME, LT_FOREIGN_KEY, RT_R_ID, RT_F1, RT_F2, NAME_ALIAS) VALUES (1, 'zero', 0, NULL, NULL, NULL, 'zero');
ASSERT VALUES `OUTPUT` (LT_L_ID, LT_NAME, LT_FOREIGN_KEY, RT_R_ID, RT_F1, RT_F2, NAME_ALIAS, ROWTIME) VALUES (1, 'zero', 0, 0, 'blah', 4, 'zero', 10000);
ASSERT VALUES `OUTPUT` (LT_L_ID, LT_NAME, LT_FOREIGN_KEY, RT_R_ID, RT_F1, RT_F2, NAME_ALIAS, ROWTIME) VALUES (1, 'zero', 0, 0, 'blah', 4, 'zero', 11000);
ASSERT VALUES `OUTPUT` (LT_L_ID, ROWTIME) VALUES (1, 18000);
ASSERT table OUTPUT (LT_L_ID BIGINT PRIMARY KEY, LT_NAME STRING, LT_FOREIGN_KEY BIGINT, RT_R_ID BIGINT, RT_F1 STRING, RT_F2 BIGINT, NAME_ALIAS STRING) WITH (KAFKA_TOPIC='OUTPUT');

