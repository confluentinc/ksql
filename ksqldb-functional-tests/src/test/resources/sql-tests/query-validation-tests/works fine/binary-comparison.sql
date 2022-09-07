--@test: binary-comparison - equals
CREATE STREAM INPUT (A INT KEY, B BOOLEAN, C BIGINT, D DOUBLE, E DECIMAL(4,3), F STRING, G ARRAY<INT>, H MAP<STRING, INT>, I STRUCT<ID INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, A = 1, B = true, C = 11, D = 1.1, E = 1.20, F = 'foo', G = ARRAY[1,2], H = MAP('a':=1), I = STRUCT(ID:=2) FROM INPUT;
INSERT INTO `INPUT` (A, B, C, D, E, F, G, H, I) VALUES (1, true, 11, 1.1, 1.20, 'foo', ARRAY[1, 2], MAP('a':=1), STRUCT(id:=2));
INSERT INTO `INPUT` (A, B, C, D, E, F, G, H, I) VALUES (2, false, 10, 1.0, 1.21, 'Foo', ARRAY[1], MAP('b':=1), STRUCT(id:=3));
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4, KSQL_COL_5, KSQL_COL_6, KSQL_COL_7, KSQL_COL_8) VALUES (1, true, true, true, true, true, true, true, true, true);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4, KSQL_COL_5, KSQL_COL_6, KSQL_COL_7, KSQL_COL_8) VALUES (2, false, false, false, false, false, false, false, false, false);

--@test: binary-comparison - not equals
CREATE STREAM INPUT (A INT KEY, B BOOLEAN, C BIGINT, D DOUBLE, E DECIMAL(4,3), F STRING, G ARRAY<INT>, H MAP<STRING, INT>, I STRUCT<ID INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, A <> 1, B <> true, C <> 11, D <> 1.1, E <> 1.20, F <> 'foo', G <> ARRAY[1,2], H <> MAP('a':=1), I <> STRUCT(ID:=2) FROM INPUT;
INSERT INTO `INPUT` (A, B, C, D, E, F, G, H, I) VALUES (1, true, 11, 1.1, 1.20, 'foo', ARRAY[1, 2], MAP('a':=1), STRUCT(id:=2));
INSERT INTO `INPUT` (A, B, C, D, E, F, G, H, I) VALUES (2, false, 10, 1.0, 1.21, 'Foo', ARRAY[1], MAP('b':=1), STRUCT(id:=3));
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4, KSQL_COL_5, KSQL_COL_6, KSQL_COL_7, KSQL_COL_8) VALUES (1, false, false, false, false, false, false, false, false, false);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4, KSQL_COL_5, KSQL_COL_6, KSQL_COL_7, KSQL_COL_8) VALUES (2, true, true, true, true, true, true, true, true, true);

--@test: binary-comparison - less than
CREATE STREAM INPUT (A INT KEY, B BIGINT, C DOUBLE, D DECIMAL(4,3), E STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, A < 1, B < 11, C < 1.1, D < 1.20, E < 'foo' FROM INPUT;
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (0, 10, 1.0, 1.19, 'Foo');
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (1, 11, 1.1, 1.20, 'foo');
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (0, true, true, true, true, true);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (1, false, false, false, false, false);

--@test: binary-comparison - less than or equal
CREATE STREAM INPUT (A INT KEY, B BIGINT, C DOUBLE, D DECIMAL(4,3), E STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, A <= 1, B <= 11, C <= 1.1, D <= 1.20, E <= 'foo' FROM INPUT;
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (0, 10, 1.0, 1.19, 'Foo');
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (1, 11, 1.1, 1.20, 'foo');
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (2, 12, 1.11, 1.21, 'goo');
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (0, true, true, true, true, true);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (1, true, true, true, true, true);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (2, false, false, false, false, false);

--@test: binary-comparison - greater than
CREATE STREAM INPUT (A INT KEY, B BIGINT, C DOUBLE, D DECIMAL(4,3), E STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, A > 1, B > 11, C > 1.1, D > 1.20, E > 'foo' FROM INPUT;
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (1, 11, 1.1, 1.20, 'foo');
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (2, 12, 1.11, 1.21, 'goo');
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (1, false, false, false, false, false);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (2, true, true, true, true, true);

--@test: binary-comparison - greater than or equal
CREATE STREAM INPUT (A INT KEY, B BIGINT, C DOUBLE, D DECIMAL(4,3), E STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, A >= 1, B >= 11, C >= 1.1, D >= 1.20, E >= 'foo' FROM INPUT;
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (0, 10, 1.0, 1.19, 'Foo');
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (1, 11, 1.1, 1.20, 'foo');
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (2, 12, 1.11, 1.21, 'goo');
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (0, false, false, false, false, false);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (1, true, true, true, true, true);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (2, true, true, true, true, true);

--@test: binary-comparison - between
CREATE STREAM INPUT (A INT KEY, B BIGINT, C DOUBLE, D DECIMAL(4,3), E STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, A BETWEEN 0 AND 2, B BETWEEN 10 AND 12, C BETWEEN 1.0 AND 1.11, D BETWEEN 1.19 AND 1.21, E BETWEEN 'eoo' AND 'goo' FROM INPUT;
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (-1, 9, 0.99, 1.18, 'doo');
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (0, 10, 1.0, 1.19, 'eoo');
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (1, 11, 1.1, 1.20, 'foo');
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (2, 12, 1.11, 1.21, 'goo');
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (3, 13, 1.12, 1.22, 'hoo');
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (-1, false, false, false, false, false);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (0, true, true, true, true, true);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (1, true, true, true, true, true);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (2, true, true, true, true, true);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (3, false, false, false, false, false);

--@test: binary-comparison - not between
CREATE STREAM INPUT (A INT KEY, B BIGINT, C DOUBLE, D DECIMAL(4,3), E STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, A NOT BETWEEN 0 AND 2, B NOT BETWEEN 10 AND 12, C NOT BETWEEN 1.0 AND 1.11, D NOT BETWEEN 1.19 AND 1.21, E NOT BETWEEN 'eoo' AND 'goo' FROM INPUT;
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (-1, 9, 0.99, 1.18, 'doo');
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (0, 10, 1.0, 1.19, 'eoo');
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (1, 11, 1.1, 1.20, 'foo');
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (2, 12, 1.11, 1.21, 'goo');
INSERT INTO `INPUT` (A, B, C, D, E) VALUES (3, 13, 1.12, 1.22, 'hoo');
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (-1, true, true, true, true, true);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (0, false, false, false, false, false);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (1, false, false, false, false, false);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (2, false, false, false, false, false);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (3, true, true, true, true, true);

--@test: binary-comparison - is distinct from
CREATE STREAM INPUT (ID INT KEY, ID2 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, ID IS DISTINCT FROM ID2 FROM INPUT;
INSERT INTO `INPUT` (ID, ID2) VALUES (1, 1);
INSERT INTO `INPUT` (ID, ID2) VALUES (2, 1);
INSERT INTO `INPUT` (ID, ID2) VALUES (3, NULL);
INSERT INTO `INPUT` (ID2) VALUES (1);
INSERT INTO `INPUT` (ID2) VALUES (NULL);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES (1, false);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES (2, true);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES (3, true);
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (true);
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (false);

--@test: binary-comparison - is distinct from (2)
CREATE STREAM INPUT (A INT KEY, B BOOLEAN, C BIGINT, D DOUBLE, E DECIMAL(4,3), F STRING, G ARRAY<INT>, H MAP<STRING, INT>, I STRUCT<ID INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, A IS DISTINCT FROM 1, B IS DISTINCT FROM true, C IS DISTINCT FROM 11, D IS DISTINCT FROM 1.1, E IS DISTINCT FROM 1.20, F IS DISTINCT FROM 'foo', G IS DISTINCT FROM ARRAY[1,2], H IS DISTINCT FROM MAP('a':=1), I IS DISTINCT FROM STRUCT(ID:=2) FROM INPUT;
INSERT INTO `INPUT` (A, B, C, D, E, F, G, H, I) VALUES (1, true, 11, 1.1, 1.20, 'foo', ARRAY[1, 2], MAP('a':=1), STRUCT(id:=2));
INSERT INTO `INPUT` (A, B, C, D, E, F, G, H, I) VALUES (2, false, 10, 1.0, 1.21, 'Foo', ARRAY[1], MAP('b':=1), STRUCT(id:=3));
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4, KSQL_COL_5, KSQL_COL_6, KSQL_COL_7, KSQL_COL_8) VALUES (1, false, false, false, false, false, false, false, false, false);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4, KSQL_COL_5, KSQL_COL_6, KSQL_COL_7, KSQL_COL_8) VALUES (2, true, true, true, true, true, true, true, true, true);

--@test: binary-comparison - is not distinct from
CREATE STREAM INPUT (ID INT KEY, ID2 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, ID IS NOT DISTINCT FROM ID2 FROM INPUT;
INSERT INTO `INPUT` (ID, ID2) VALUES (1, 1);
INSERT INTO `INPUT` (ID, ID2) VALUES (2, 1);
INSERT INTO `INPUT` (ID, ID2) VALUES (3, NULL);
INSERT INTO `INPUT` (ID2) VALUES (1);
INSERT INTO `INPUT` (ID2) VALUES (NULL);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES (1, true);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES (2, false);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES (3, false);
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (false);
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (true);

--@test: binary-comparison - is not distinct from (2)
CREATE STREAM INPUT (A INT KEY, B BOOLEAN, C BIGINT, D DOUBLE, E DECIMAL(4,3), F STRING, G ARRAY<INT>, H MAP<STRING, INT>, I STRUCT<ID INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, A IS NOT DISTINCT FROM 1, B IS NOT DISTINCT FROM true, C IS NOT DISTINCT FROM 11, D IS NOT DISTINCT FROM 1.1, E IS NOT DISTINCT FROM 1.20, F IS NOT DISTINCT FROM 'foo', G IS NOT DISTINCT FROM ARRAY[1,2], H IS NOT DISTINCT FROM MAP('a':=1), I IS NOT DISTINCT FROM STRUCT(ID:=2) FROM INPUT;
INSERT INTO `INPUT` (A, B, C, D, E, F, G, H, I) VALUES (1, true, 11, 1.1, 1.20, 'foo', ARRAY[1, 2], MAP('a':=1), STRUCT(id:=2));
INSERT INTO `INPUT` (A, B, C, D, E, F, G, H, I) VALUES (2, false, 10, 1.0, 1.21, 'Foo', ARRAY[1], MAP('b':=1), STRUCT(id:=3));
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4, KSQL_COL_5, KSQL_COL_6, KSQL_COL_7, KSQL_COL_8) VALUES (1, true, true, true, true, true, true, true, true, true);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4, KSQL_COL_5, KSQL_COL_6, KSQL_COL_7, KSQL_COL_8) VALUES (2, false, false, false, false, false, false, false, false, false);

--@test: binary-comparison - array comparison fails
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Cannot compare B (ARRAY<INTEGER>) to C (ARRAY<INTEGER>) with LESS_THAN.
CREATE STREAM INPUT (A INT KEY, B ARRAY<INT>, C ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, B < C, B > C FROM INPUT;
--@test: binary-comparison - array equality
CREATE STREAM INPUT (A INT KEY, B ARRAY<INT>, C ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, B = C, B <> C, B IS NOT DISTINCT FROM C, B IS DISTINCT FROM C FROM INPUT;
INSERT INTO `INPUT` (A, B, C) VALUES (1, ARRAY[1, 2], ARRAY[1, 2]);
INSERT INTO `INPUT` (A, B, C) VALUES (2, ARRAY[1, 2], ARRAY[1]);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES (1, true, false, true, false);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES (2, false, true, false, true);

--@test: binary-comparison - map comparison fails
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Cannot compare B (MAP<STRING, INTEGER>) to C (MAP<STRING, INTEGER>) with LESS_THAN.
CREATE STREAM INPUT (A INT KEY, B MAP<STRING, INT>, C MAP<STRING, INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, B < C, B > C FROM INPUT;
--@test: binary-comparison - map equality
CREATE STREAM INPUT (A INT KEY, B MAP<STRING, INT>, C MAP<STRING, INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, B = C, B <> C, B IS NOT DISTINCT FROM C, B IS DISTINCT FROM C FROM INPUT;
INSERT INTO `INPUT` (A, B, C) VALUES (1, MAP('a':=1), MAP('a':=1));
INSERT INTO `INPUT` (A, B, C) VALUES (2, MAP('a':=1), MAP('a':=2));
INSERT INTO `INPUT` (A, B, C) VALUES (3, MAP('a':=1), MAP('b':=1));
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES (1, true, false, true, false);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES (2, false, true, false, true);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES (3, false, true, false, true);

--@test: binary-comparison - struct comparison fails
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Cannot compare B (STRUCT<`ID` INTEGER>) to C (STRUCT<`ID` INTEGER>) with LESS_THAN.
CREATE STREAM INPUT (A INT KEY, B STRUCT<ID INT>, C STRUCT<ID INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, B < C, B > C FROM INPUT;
--@test: binary-comparison - struct equality
CREATE STREAM INPUT (A INT KEY, B STRUCT<ID INT>, C STRUCT<ID INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT A, B = C, B <> C, B IS NOT DISTINCT FROM C, B IS DISTINCT FROM C FROM INPUT;
INSERT INTO `INPUT` (A, B, C) VALUES (1, STRUCT(id:=2), STRUCT(id:=2));
INSERT INTO `INPUT` (A, B, C) VALUES (2, STRUCT(id:=2), STRUCT(id:=1));
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES (1, true, false, true, false);
ASSERT VALUES `OUTPUT` (A, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES (2, false, true, false, true);

