--@test: fk-n-way-join - Should fail as second step in n-way join
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: foreign-key table-table joins are not supported as part of n-way joins. Got LEFT_TABLE.F1 = RIGHT_TABLE.ID3.
CREATE TABLE left_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE middle_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='middle_topic', format='JSON');
CREATE TABLE right_table (id3 BIGINT PRIMARY KEY, f3 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, f1, f2, f3 FROM left_table JOIN middle_table ON id1 = id2 JOIN right_table ON f1 = id3;
--@test: fk-n-way-join - Should allow fk join at start of n-way join
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, foreign_key BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE middle_table (m_id BIGINT PRIMARY KEY, f2 BIGINT, other STRING) WITH (kafka_topic='middle_topic', format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f3 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT l_id, m_id, foreign_key, f2, f3 FROM left_table JOIN middle_table ON foreign_key = m_id LEFT JOIN right_table ON l_id = r_id;
INSERT INTO `MIDDLE_TABLE` (M_ID, F2, OTHER, ROWTIME) VALUES (0, 100, 'unused', 0);
INSERT INTO `LEFT_TABLE` (L_ID, FOREIGN_KEY, ROWTIME) VALUES (1, 0, 10000);
INSERT INTO `RIGHT_TABLE` (R_ID, F3, ROWTIME) VALUES (1, 4, 11000);
INSERT INTO `MIDDLE_TABLE` (M_ID, F2, OTHER, ROWTIME) VALUES (8, 10, 'unused', 13000);
INSERT INTO `LEFT_TABLE` (L_ID, FOREIGN_KEY, ROWTIME) VALUES (1, 8, 16000);
INSERT INTO `LEFT_TABLE` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (L_ID, M_ID, FOREIGN_KEY, F2, F3, ROWTIME) VALUES (1, 0, 0, 100, NULL, 10000);
ASSERT VALUES `OUTPUT` (L_ID, M_ID, FOREIGN_KEY, F2, F3, ROWTIME) VALUES (1, 0, 0, 100, 4, 11000);
ASSERT VALUES `OUTPUT` (L_ID, M_ID, FOREIGN_KEY, F2, F3, ROWTIME) VALUES (1, 8, 8, 10, 4, 16000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT table OUTPUT (L_ID BIGINT PRIMARY KEY, M_ID BIGINT, FOREIGN_KEY BIGINT, F2 BIGINT, F3 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-n-way-join - Should fail as second step in n-way join with fk join as first step
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: foreign-key table-table joins are not supported as part of n-way joins. Got LEFT_TABLE.F1 = RIGHT_TABLE.ID3.
CREATE TABLE left_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE middle_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='middle_topic', format='JSON');
CREATE TABLE right_table (id3 BIGINT PRIMARY KEY, f3 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, f1, f2, f3 FROM left_table JOIN middle_table ON f1 = id2 JOIN right_table ON f1 = id3;
--@test: fk-n-way-join - Should allow fk join at start of n-way join - without fk join expressions in projection
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, foreign_key BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE middle_table (m_id BIGINT PRIMARY KEY, f2 BIGINT, other STRING) WITH (kafka_topic='middle_topic', format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f3 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT l_id, f2, f3 FROM left_table JOIN middle_table ON foreign_key = m_id LEFT JOIN right_table ON l_id = r_id;
INSERT INTO `MIDDLE_TABLE` (M_ID, F2, OTHER, ROWTIME) VALUES (0, 100, 'unused', 0);
INSERT INTO `LEFT_TABLE` (L_ID, FOREIGN_KEY, ROWTIME) VALUES (1, 0, 10000);
INSERT INTO `RIGHT_TABLE` (R_ID, F3, ROWTIME) VALUES (1, 4, 11000);
INSERT INTO `MIDDLE_TABLE` (M_ID, F2, OTHER, ROWTIME) VALUES (8, 10, 'unused', 13000);
INSERT INTO `LEFT_TABLE` (L_ID, FOREIGN_KEY, ROWTIME) VALUES (1, 8, 16000);
INSERT INTO `LEFT_TABLE` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (L_ID, F2, F3, ROWTIME) VALUES (1, 100, NULL, 10000);
ASSERT VALUES `OUTPUT` (L_ID, F2, F3, ROWTIME) VALUES (1, 100, 4, 11000);
ASSERT VALUES `OUTPUT` (L_ID, F2, F3, ROWTIME) VALUES (1, 10, 4, 16000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT table OUTPUT (L_ID BIGINT PRIMARY KEY, F2 BIGINT, F3 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-n-way-join - FK join at start of join should fail without key in projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the join expressions LEFT_TABLE.ID1 or RIGHT_TABLE.ID3 in its projection (eg, SELECT LEFT_TABLE.ID1...).
CREATE TABLE left_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE middle_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='middle_topic', format='JSON');
CREATE TABLE right_table (id3 BIGINT PRIMARY KEY, f3 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT f1, f2, f3 FROM left_table JOIN middle_table ON f1 = id2 JOIN right_table ON id1 = id3;
--@test: fk-n-way-join - Should allow fk join at start of n-way join - alternative key expression in projection
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, foreign_key BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE middle_table (m_id BIGINT PRIMARY KEY, f2 BIGINT, other STRING) WITH (kafka_topic='middle_topic', format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f3 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT r_id, m_id, foreign_key, f2, f3 FROM left_table JOIN middle_table ON foreign_key = m_id LEFT JOIN right_table ON l_id = r_id;
INSERT INTO `MIDDLE_TABLE` (M_ID, F2, OTHER, ROWTIME) VALUES (0, 100, 'unused', 0);
INSERT INTO `LEFT_TABLE` (L_ID, FOREIGN_KEY, ROWTIME) VALUES (1, 0, 10000);
INSERT INTO `RIGHT_TABLE` (R_ID, F3, ROWTIME) VALUES (1, 4, 11000);
INSERT INTO `MIDDLE_TABLE` (M_ID, F2, OTHER, ROWTIME) VALUES (8, 10, 'unused', 13000);
INSERT INTO `LEFT_TABLE` (L_ID, FOREIGN_KEY, ROWTIME) VALUES (1, 8, 16000);
INSERT INTO `LEFT_TABLE` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (R_ID, M_ID, FOREIGN_KEY, F2, F3, ROWTIME) VALUES (1, 0, 0, 100, NULL, 10000);
ASSERT VALUES `OUTPUT` (R_ID, M_ID, FOREIGN_KEY, F2, F3, ROWTIME) VALUES (1, 0, 0, 100, 4, 11000);
ASSERT VALUES `OUTPUT` (R_ID, M_ID, FOREIGN_KEY, F2, F3, ROWTIME) VALUES (1, 8, 8, 10, 4, 16000);
ASSERT VALUES `OUTPUT` (R_ID, ROWTIME) VALUES (1, 18000);
ASSERT table OUTPUT (R_ID BIGINT PRIMARY KEY, M_ID BIGINT, FOREIGN_KEY BIGINT, F2 BIGINT, F3 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-n-way-join - Should allow fk join at start of n-way join - select *
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, foreign_key BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE middle_table (m_id BIGINT PRIMARY KEY, f2 BIGINT, other STRING) WITH (kafka_topic='middle_topic', format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f3 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT * FROM left_table JOIN middle_table ON foreign_key = m_id LEFT JOIN right_table ON l_id = r_id;
INSERT INTO `MIDDLE_TABLE` (M_ID, F2, OTHER, ROWTIME) VALUES (0, 100, 'unused', 0);
INSERT INTO `LEFT_TABLE` (L_ID, FOREIGN_KEY, ROWTIME) VALUES (1, 0, 10000);
INSERT INTO `RIGHT_TABLE` (R_ID, F3, ROWTIME) VALUES (1, 4, 11000);
INSERT INTO `MIDDLE_TABLE` (M_ID, F2, OTHER, ROWTIME) VALUES (8, 10, 'unused', 13000);
INSERT INTO `LEFT_TABLE` (L_ID, FOREIGN_KEY, ROWTIME) VALUES (1, 8, 16000);
INSERT INTO `LEFT_TABLE` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (LEFT_TABLE_L_ID, LEFT_TABLE_FOREIGN_KEY, MIDDLE_TABLE_M_ID, MIDDLE_TABLE_F2, MIDDLE_TABLE_OTHER, RIGHT_TABLE_R_ID, RIGHT_TABLE_F3, ROWTIME) VALUES (1, 0, 0, 100, 'unused', NULL, NULL, 10000);
ASSERT VALUES `OUTPUT` (LEFT_TABLE_L_ID, LEFT_TABLE_FOREIGN_KEY, MIDDLE_TABLE_M_ID, MIDDLE_TABLE_F2, MIDDLE_TABLE_OTHER, RIGHT_TABLE_R_ID, RIGHT_TABLE_F3, ROWTIME) VALUES (1, 0, 0, 100, 'unused', 1, 4, 11000);
ASSERT VALUES `OUTPUT` (LEFT_TABLE_L_ID, LEFT_TABLE_FOREIGN_KEY, MIDDLE_TABLE_M_ID, MIDDLE_TABLE_F2, MIDDLE_TABLE_OTHER, RIGHT_TABLE_R_ID, RIGHT_TABLE_F3, ROWTIME) VALUES (1, 8, 8, 10, 'unused', 1, 4, 16000);
ASSERT VALUES `OUTPUT` (LEFT_TABLE_L_ID, ROWTIME) VALUES (1, 18000);
ASSERT table OUTPUT (LEFT_TABLE_L_ID BIGINT PRIMARY KEY, LEFT_TABLE_FOREIGN_KEY BIGINT, MIDDLE_TABLE_M_ID BIGINT, MIDDLE_TABLE_F2 BIGINT, MIDDLE_TABLE_OTHER VARCHAR, RIGHT_TABLE_R_ID BIGINT, RIGHT_TABLE_F3 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-n-way-join - Should allow fk join at start of n-way join - qualified select *
CREATE TABLE left_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE middle_table (id2 BIGINT PRIMARY KEY, f2 BIGINT, other STRING) WITH (kafka_topic='middle_topic', format='JSON');
CREATE TABLE right_table (id3 BIGINT PRIMARY KEY, f3 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, middle_table.*, right_table.* FROM left_table JOIN middle_table ON f1 = id2 LEFT JOIN right_table ON id1 = id3;
INSERT INTO `MIDDLE_TABLE` (ID2, F2, OTHER, ROWTIME) VALUES (0, 100, 'foo', 0);
INSERT INTO `LEFT_TABLE` (ID1, F1, ROWTIME) VALUES (1, 0, 10000);
INSERT INTO `RIGHT_TABLE` (ID3, F3, ROWTIME) VALUES (1, 4, 11000);
INSERT INTO `MIDDLE_TABLE` (ID2, F2, OTHER, ROWTIME) VALUES (8, 10, 'bar', 13000);
INSERT INTO `LEFT_TABLE` (ID1, F1, ROWTIME) VALUES (1, 8, 16000);
INSERT INTO `LEFT_TABLE` (ID1, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (ID1, MIDDLE_TABLE_ID2, MIDDLE_TABLE_F2, MIDDLE_TABLE_OTHER, RIGHT_TABLE_ID3, RIGHT_TABLE_F3, ROWTIME) VALUES (1, 0, 100, 'foo', NULL, NULL, 10000);
ASSERT VALUES `OUTPUT` (ID1, MIDDLE_TABLE_ID2, MIDDLE_TABLE_F2, MIDDLE_TABLE_OTHER, RIGHT_TABLE_ID3, RIGHT_TABLE_F3, ROWTIME) VALUES (1, 0, 100, 'foo', 1, 4, 11000);
ASSERT VALUES `OUTPUT` (ID1, MIDDLE_TABLE_ID2, MIDDLE_TABLE_F2, MIDDLE_TABLE_OTHER, RIGHT_TABLE_ID3, RIGHT_TABLE_F3, ROWTIME) VALUES (1, 8, 10, 'bar', 1, 4, 16000);
ASSERT VALUES `OUTPUT` (ID1, ROWTIME) VALUES (1, 18000);
ASSERT table OUTPUT (ID1 BIGINT PRIMARY KEY, MIDDLE_TABLE_ID2 BIGINT, MIDDLE_TABLE_F2 BIGINT, MIDDLE_TABLE_OTHER STRING, RIGHT_TABLE_ID3 BIGINT, RIGHT_TABLE_F3 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: fk-n-way-join - Should allow fk join at start of n-way join - with aliases
CREATE TABLE left_table (l_id BIGINT PRIMARY KEY, foreign_key BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE middle_table (m_id BIGINT PRIMARY KEY, f2 BIGINT, other STRING) WITH (kafka_topic='middle_topic', format='JSON');
CREATE TABLE right_table (r_id BIGINT PRIMARY KEY, f3 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT l_id, m_id AS m_id_alias, foreign_key, mt.f2 AS mt_f2_alias, rt.f3 FROM left_table AS lt JOIN middle_table AS mt ON foreign_key = mt.m_id LEFT JOIN right_table AS rt ON lt.l_id = r_id;
INSERT INTO `MIDDLE_TABLE` (M_ID, F2, OTHER, ROWTIME) VALUES (0, 100, 'unused', 0);
INSERT INTO `LEFT_TABLE` (L_ID, FOREIGN_KEY, ROWTIME) VALUES (1, 0, 10000);
INSERT INTO `RIGHT_TABLE` (R_ID, F3, ROWTIME) VALUES (1, 4, 11000);
INSERT INTO `MIDDLE_TABLE` (M_ID, F2, OTHER, ROWTIME) VALUES (8, 10, 'unused', 13000);
INSERT INTO `LEFT_TABLE` (L_ID, FOREIGN_KEY, ROWTIME) VALUES (1, 8, 16000);
INSERT INTO `LEFT_TABLE` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT VALUES `OUTPUT` (L_ID, M_ID_ALIAS, FOREIGN_KEY, MT_F2_ALIAS, F3, ROWTIME) VALUES (1, 0, 0, 100, NULL, 10000);
ASSERT VALUES `OUTPUT` (L_ID, M_ID_ALIAS, FOREIGN_KEY, MT_F2_ALIAS, F3, ROWTIME) VALUES (1, 0, 0, 100, 4, 11000);
ASSERT VALUES `OUTPUT` (L_ID, M_ID_ALIAS, FOREIGN_KEY, MT_F2_ALIAS, F3, ROWTIME) VALUES (1, 8, 8, 10, 4, 16000);
ASSERT VALUES `OUTPUT` (L_ID, ROWTIME) VALUES (1, 18000);
ASSERT table OUTPUT (L_ID BIGINT PRIMARY KEY, M_ID_ALIAS BIGINT, FOREIGN_KEY BIGINT, MT_F2_ALIAS BIGINT, F3 BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

