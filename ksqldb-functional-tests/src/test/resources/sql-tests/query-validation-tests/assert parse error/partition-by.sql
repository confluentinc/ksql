--@test: partition-by - no key column
CREATE STREAM INPUT (NAME STRING, ID INT) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS select ID, NAME from INPUT partition by ID;
INSERT INTO `INPUT` (NAME, ID) VALUES ('bob', 10);
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (10, 'bob');
ASSERT stream OUTPUT (ID INT KEY, NAME STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - struct field - select star
CREATE STREAM INPUT (ID INT KEY, ADDRESS STRUCT<STREET STRING, TOWN STRING>, AGE INT) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS select * from INPUT partition by ADDRESS->TOWN;
INSERT INTO `INPUT` (ID, ADDRESS, AGE) VALUES (10, STRUCT(STREET:='1st Steet', Town:='Oxford'), 22);
ASSERT VALUES `OUTPUT` (TOWN, ID, ADDRESS, AGE) VALUES ('Oxford', 10, STRUCT(STREET:='1st Steet', TOWN:='Oxford'), 22);
ASSERT stream OUTPUT (TOWN STRING KEY, ADDRESS STRUCT<STREET STRING, TOWN STRING>, AGE INT, ID INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - struct field - select explicit
CREATE STREAM INPUT (ID INT KEY, ADDRESS STRUCT<STREET STRING, TOWN STRING>, AGE INT) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS select ID, AGE, ADDRESS->TOWN from INPUT partition by ADDRESS->TOWN;
INSERT INTO `INPUT` (ID, ADDRESS, AGE) VALUES (10, STRUCT(STREET:='1st Steet', Town:='Oxford'), 22);
ASSERT VALUES `OUTPUT` (TOWN, ID, AGE) VALUES ('Oxford', 10, 22);
ASSERT stream OUTPUT (TOWN STRING KEY, ID INT, AGE INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - struct field - with alias
CREATE STREAM INPUT (ID INT KEY, ADDRESS STRUCT<STREET STRING, TOWN STRING>, AGE INT) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS select ADDRESS->TOWN AS K, ID, AGE from INPUT partition by ADDRESS->TOWN;
INSERT INTO `INPUT` (ID, ADDRESS, AGE) VALUES (10, STRUCT(STREET:='1st Steet', Town:='Oxford'), 22);
ASSERT VALUES `OUTPUT` (K, ID, AGE) VALUES ('Oxford', 10, 22);
ASSERT stream OUTPUT (K STRING KEY, ID INT, AGE INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - null - explicit key format
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Key format specified for stream without key columns.
CREATE STREAM INPUT (ID INT KEY, NAME STRING) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT WITH(key_format='DELIMITED') AS SELECT ID, NAME from INPUT partition by null;
--@test: partition-by - key in projection more than once
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The projection contains a key column (`NAME`) more than once, aliased as: NAME and NAME2.
CREATE STREAM INPUT (ID INT KEY, NAME STRING) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS select NAME, NAME AS NAME2, ID from INPUT partition by NAME;
--@test: partition-by - expression - missing key from projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the partitioning expression ABS(INPUT.ID) in its projection (eg, SELECT ABS(INPUT.ID)...).
CREATE STREAM INPUT (ID INT KEY, NAME STRING) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS select NAME, ID from INPUT partition by ABS(ID);
--@test: partition-by - expression - missing key from projection - with value column of same name
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the partitioning expression INPUT.ID in its projection (eg, SELECT INPUT.ID...).
CREATE STREAM INPUT (ID INT KEY, NAME STRING) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS select NAME AS ID, NAME from INPUT partition by ID;
--@test: partition-by - only key column - select star - with join on keys
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (X INT KEY, Y INT, Z INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM L JOIN R WITHIN 10 SECONDS ON L.A = R.X PARTITION BY L.B;
INSERT INTO `L` (A, B, C, ROWTIME) VALUES (0, 1, 2, 11);
INSERT INTO `R` (X, Y, Z, ROWTIME) VALUES (0, -1, -2, 12);
ASSERT VALUES `OUTPUT` (L_B, L_ROWTIME, L_ROWPARTITION, L_ROWOFFSET, R_ROWTIME, R_ROWPARTITION, R_ROWOFFSET, L_A, R_X, R_Y, L_C, R_Z) VALUES (1, 11, 0, 0, 12, 0, 0, 0, 0, -1, 2, -2);
ASSERT stream OUTPUT (L_B INT KEY, L_C INT, L_ROWTIME BIGINT, L_ROWPARTITION INT, L_ROWOFFSET BIGINT, L_A INT, R_Y INT, R_Z INT, R_ROWTIME BIGINT, R_ROWPARTITION INT, R_ROWOFFSET BIGINT, R_X INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - only key column - select star - with join on value columns
CREATE STREAM L (A INT KEY, B INT, C INT) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (X INT KEY, Y INT, Z INT) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM L JOIN R WITHIN 10 SECONDS ON L.B = R.Y PARTITION BY A;
INSERT INTO `L` (A, B, C, ROWTIME) VALUES (0, 1, 2, 11);
INSERT INTO `R` (X, Y, Z, ROWTIME) VALUES (-1, 1, -2, 12);
ASSERT VALUES `OUTPUT` (L_A, L_ROWTIME, L_ROWPARTITION, L_ROWOFFSET, R_ROWTIME, R_ROWPARTITION, R_ROWOFFSET, R_X, L_B, R_Y, L_C, R_Z) VALUES (0, 11, 0, 0, 12, 0, 0, -1, 1, 1, 2, -2);
ASSERT stream OUTPUT (L_A INT KEY, L_B INT, L_C INT, L_ROWTIME BIGINT,L_ROWPARTITION INT, L_ROWOFFSET BIGINT, R_Y INT, R_Z INT, R_ROWTIME BIGINT, R_ROWPARTITION INT, R_ROWOFFSET BIGINT, R_X INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - nulls
CREATE STREAM TEST (K BIGINT KEY, ID bigint, NAME varchar) with (kafka_topic='test_topic', value_format = 'delimited');
CREATE STREAM REPARTITIONED AS select name, id from TEST partition by name;
INSERT INTO `TEST` (K) VALUES (1);
INSERT INTO `TEST` (K, ID, NAME) VALUES (2, 4, NULL);
INSERT INTO `TEST` (K, ID, NAME) VALUES (3, 5, 'zero');
ASSERT VALUES `REPARTITIONED` (NAME, ID) VALUES (NULL, NULL);
ASSERT VALUES `REPARTITIONED` (ID) VALUES (4);
ASSERT VALUES `REPARTITIONED` (NAME, ID) VALUES ('zero', 5);

--@test: partition-by - single non-key column - with alias that matches old key column
CREATE STREAM INPUT (ID INT KEY, NAME STRING, AGE INT) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS select NAME AS ID, AGE, ID AS OLD from INPUT partition by NAME;
INSERT INTO `INPUT` (ID, NAME, AGE) VALUES (10, 'bob', 22);
ASSERT VALUES `OUTPUT` (ID, OLD, AGE) VALUES ('bob', 10, 22);
ASSERT stream OUTPUT (ID STRING KEY, AGE INT, OLD INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - with alias that matches source column
CREATE STREAM INPUT (ID INT KEY, NAME STRING, AGE INT) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS select NAME AS AGE, ID from INPUT partition by NAME;
INSERT INTO `INPUT` (ID, NAME, AGE) VALUES (10, 'bob', 22);
ASSERT VALUES `OUTPUT` (AGE, ID) VALUES ('bob', 10);
ASSERT stream OUTPUT (AGE STRING KEY, ID INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - complex expressions
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: mismatched input 'AND' expecting {';'
CREATE STREAM INPUT (ID INT KEY, NAME STRING, AGE INT) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS select * from INPUT partition by NAME AND AGE;
--@test: partition-by - partition by with projection select some
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) with (kafka_topic='test_topic', value_format = 'delimited');
CREATE STREAM REPARTITIONED AS select name, id from TEST partition by name;
INSERT INTO `TEST` (K, ID, NAME, VALUE) VALUES ('0', 0, 'zero', 50);
ASSERT VALUES `REPARTITIONED` (NAME, ID) VALUES ('zero', 0);
ASSERT stream REPARTITIONED (NAME STRING KEY, ID BIGINT) WITH (KAFKA_TOPIC='REPARTITIONED');

--@test: partition-by - int column
CREATE STREAM TEST (K STRING KEY, ID bigint) with (kafka_topic='test_topic', value_format = 'delimited');
CREATE STREAM OUTPUT AS SELECT * from TEST partition by id;
INSERT INTO `TEST` (K, ID) VALUES ('a', 10);
ASSERT VALUES `OUTPUT` (ID, K) VALUES (10, 'a');
ASSERT stream OUTPUT (ID BIGINT KEY, K STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - bigint key field
CREATE STREAM TEST (K BIGINT KEY, ID BIGINT) with (kafka_topic='test_topic', value_format = 'delimited');
CREATE STREAM OUTPUT AS select * from TEST partition by ID;
INSERT INTO `TEST` (K, ID) VALUES (0, 0);
ASSERT VALUES `OUTPUT` (ID, K) VALUES (0, 0);
ASSERT stream OUTPUT (ID BIGINT KEY, K BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - partition by - KAFKA
CREATE STREAM INPUT (K STRING KEY, ID int) with (kafka_topic='input', value_format = 'KAFKA');
CREATE STREAM OUTPUT AS select ID, K from INPUT partition by ID;
INSERT INTO `INPUT` (K, ID) VALUES ('0', 10);
ASSERT VALUES `OUTPUT` (ID, K) VALUES (10, '0');
ASSERT stream OUTPUT (ID INT KEY, K STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - with STRING -> BIGINT cast
CREATE STREAM INPUT (K STRING KEY, V0 STRING) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS select * from INPUT partition by CAST(V0 AS BIGINT);
INSERT INTO `INPUT` (K, V0) VALUES ('a', '10');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, V0, K) VALUES (10, '10', 'a');

--@test: partition-by - partition by with projection select all
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) with (kafka_topic='test_topic', value_format = 'delimited');
CREATE STREAM REPARTITIONED AS select * from TEST partition by name;
INSERT INTO `TEST` (K, ID, NAME, VALUE) VALUES ('1', 0, 'zero', 50);
ASSERT VALUES `REPARTITIONED` (NAME, ID, VALUE, K) VALUES ('zero', 0, 50, '1');

--@test: partition-by - partition by with null value
CREATE STREAM TEST (K BIGINT KEY, ID bigint, NAME varchar, VALUE bigint) with (kafka_topic='test_topic', value_format = 'delimited');
CREATE STREAM REPARTITIONED AS select * from TEST partition by name;
INSERT INTO `TEST` (K) VALUES (0);
INSERT INTO `TEST` (K, ID, NAME, VALUE) VALUES (0, 0, 'zero', 50);
ASSERT VALUES `REPARTITIONED` (NAME, ID, VALUE, K) VALUES (NULL, NULL, NULL, 0);
ASSERT VALUES `REPARTITIONED` (NAME, ID, VALUE, K) VALUES ('zero', 0, 50, 0);

--@test: partition-by - only key column - with null value
CREATE STREAM TEST (K BIGINT KEY, ID bigint, NAME varchar, VALUE bigint) with (kafka_topic='test_topic', value_format = 'delimited');
CREATE STREAM REPARTITIONED AS select * from TEST partition by K;
INSERT INTO `TEST` (K) VALUES (0);
INSERT INTO `TEST` (K, ID, NAME, VALUE) VALUES (0, 0, 'zero', 50);
ASSERT VALUES `REPARTITIONED` (K) VALUES (0);
ASSERT VALUES `REPARTITIONED` (K, ID, NAME, VALUE) VALUES (0, 0, 'zero', 50);

--@test: partition-by - key expression - with null value
CREATE STREAM TEST (K BIGINT KEY, ID bigint, NAME varchar, VALUE bigint) with (kafka_topic='test_topic', value_format = 'delimited');
CREATE STREAM REPARTITIONED AS select K + 2, ID, NAME, VALUE from TEST partition by K + 2;
INSERT INTO `TEST` (K) VALUES (0);
INSERT INTO `TEST` (K, ID, NAME, VALUE) VALUES (0, 0, 'zero', 50);
ASSERT VALUES `REPARTITIONED` (KSQL_COL_0) VALUES (2);
ASSERT VALUES `REPARTITIONED` (KSQL_COL_0, ID, NAME, VALUE) VALUES (2, 0, 'zero', 50);

--@test: partition-by - udf key expression - with null value
CREATE STREAM TEST (K BIGINT KEY, ID bigint, NAME varchar, VALUE bigint) with (kafka_topic='test_topic', value_format = 'delimited');
CREATE STREAM REPARTITIONED AS select ABS(K), ID, NAME, VALUE from TEST partition by ABS(K);
INSERT INTO `TEST` (K) VALUES (-1);
INSERT INTO `TEST` (K, ID, NAME, VALUE) VALUES (-1, 0, 'zero', 50);
ASSERT VALUES `REPARTITIONED` (KSQL_COL_0) VALUES (1);
ASSERT VALUES `REPARTITIONED` (KSQL_COL_0, ID, NAME, VALUE) VALUES (1, 0, 'zero', 50);

--@test: partition-by - partition by with null partition by value
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE bigint) with (kafka_topic='test_topic', value_format = 'delimited');
CREATE STREAM REPARTITIONED AS select * from TEST partition by name;
INSERT INTO `TEST` (K, ID, NAME, VALUE) VALUES ('0', 0, '', 1);
INSERT INTO `TEST` (K, ID, NAME, VALUE) VALUES ('0', 0, 'zero', 50);
ASSERT VALUES `REPARTITIONED` (ID, VALUE, K) VALUES (0, 1, '0');
ASSERT VALUES `REPARTITIONED` (NAME, ID, VALUE, K) VALUES ('zero', 0, 50, '0');

--@test: partition-by - aliased key field - different name
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: PARTITION BY column 'ID_NEW' cannot be resolved.
CREATE STREAM TEST (K STRING KEY, ID varchar, NAME varchar) with (kafka_topic='test_topic', value_format = 'delimited');
CREATE STREAM REPARTITIONED AS select ID + '_new' AS ID_new, NAME from TEST partition by ID_NEW;
--@test: partition-by - partition by project key
CREATE STREAM INPUT (K STRING KEY, ID bigint) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS select ID, AS_VALUE(K) AS OLDKEY from INPUT partition by ID;
INSERT INTO `INPUT` (K, ID) VALUES ('foo', 10);
ASSERT VALUES `OUTPUT` (ID, OLDKEY) VALUES (10, 'foo');

--@test: partition-by - partition by ROWTIME
CREATE STREAM INPUT (K STRING KEY, ID bigint) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS select * from INPUT partition by ROWTIME;
INSERT INTO `INPUT` (ID, ROWTIME) VALUES (22, 10);
ASSERT VALUES `OUTPUT` (ROWTIME, ID, K, ROWTIME) VALUES (10, 22, NULL, 10);

--@test: partition-by - partition by key in join on non-key
CREATE STREAM L (K STRING KEY, A STRING, B STRING) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (K STRING KEY, C STRING, D STRING) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT L.K AS LK, R.K FROM L JOIN R WITHIN 10 SECONDS ON L.B = R.D PARTITION BY L.K;
INSERT INTO `L` (K, A, B) VALUES ('a', 'a', 'join');
INSERT INTO `R` (K, C, D) VALUES ('c', 'c', 'join');
ASSERT VALUES `OUTPUT` (LK, R_K) VALUES ('a', 'c');

--@test: partition-by - partition by non-Key in join on Key
CREATE STREAM L (K STRING KEY, A STRING, B STRING) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (K STRING KEY, C STRING, D STRING) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT L.B, L.K, R.K FROM L JOIN R WITHIN 10 SECONDS ON L.A = R.C PARTITION BY L.B;
INSERT INTO `L` (K, A, B) VALUES ('join', 'join', 'b');
INSERT INTO `R` (K, C, D) VALUES ('join', 'join', 'd');
ASSERT VALUES `OUTPUT` (B, L_K, R_K) VALUES ('b', 'join', 'join');

--@test: partition-by - partition by non-Key in join on non-Key
CREATE STREAM L (K STRING KEY, A STRING, B STRING) WITH (kafka_topic='LEFT', value_format='JSON');
CREATE STREAM R (K STRING KEY, C STRING, D STRING) WITH (kafka_topic='RIGHT', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT L.B, L.K, R.K FROM L JOIN R WITHIN 10 SECONDS ON L.B = R.D PARTITION BY L.B;
INSERT INTO `L` (K, A, B) VALUES ('a', 'a', 'join');
INSERT INTO `R` (K, C, D) VALUES ('c', 'c', 'join');
ASSERT VALUES `OUTPUT` (B, L_K, R_K) VALUES ('join', 'a', 'c');

--@test: partition-by - only key column
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT PARTITION BY ID;
INSERT INTO `INPUT` (ID, name, ROWTIME) VALUES (11, 'a', 12345);
INSERT INTO `INPUT` (ID, name, ROWTIME) VALUES (10, 'b', 12365);
INSERT INTO `INPUT` (ID, name, ROWTIME) VALUES (11, 'c', 12375);
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (11, 'a');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (10, 'b');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (11, 'c');
ASSERT stream OUTPUT (ID INT KEY, NAME STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - should handle quoted key and value
CREATE STREAM INPUT (`Key` STRING KEY, `Name` STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT *, `Name` AS `Name2` FROM INPUT PARTITION BY `Key`;
INSERT INTO `INPUT` (Key, Name, ROWTIME) VALUES ('x', 'a', 12345);
INSERT INTO `INPUT` (Key, Name, ROWTIME) VALUES ('y', 'b', 12365);
INSERT INTO `INPUT` (Key, Name, ROWTIME) VALUES ('x', 'c', 12375);
ASSERT VALUES `OUTPUT` (Key, Name, Name2) VALUES ('x', 'a', 'a');
ASSERT VALUES `OUTPUT` (Key, Name, Name2) VALUES ('y', 'b', 'b');
ASSERT VALUES `OUTPUT` (Key, Name, Name2) VALUES ('x', 'c', 'c');
ASSERT stream OUTPUT (`Key` STRING KEY, `Name` STRING, `Name2` STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - nulls using coalesce
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar) with (kafka_topic='test_topic', value_format = 'json');
CREATE STREAM REPARTITIONED AS select COALESCE(name, 'default'), id from TEST partition by COALESCE(name, 'default');
INSERT INTO `TEST` (ID, NAME) VALUES (0, 'fred');
INSERT INTO `TEST` (ID, NAME) VALUES (1, NULL);
INSERT INTO `TEST` (ID) VALUES (2);
ASSERT VALUES `REPARTITIONED` (KSQL_COL_0, ID) VALUES ('fred', 0);
ASSERT VALUES `REPARTITIONED` (KSQL_COL_0, ID) VALUES ('default', 1);
ASSERT VALUES `REPARTITIONED` (KSQL_COL_0, ID) VALUES ('default', 2);

--@test: partition-by - should handle PARTITION BY that throws
CREATE STREAM TEST (K STRING KEY, ID bigint, shouldThrow BOOLEAN) with (kafka_topic='test_topic', value_format = 'json');
CREATE STREAM REPARTITIONED AS select bad_udf(shouldThrow), ID from TEST partition by bad_udf(shouldThrow);
INSERT INTO `TEST` (K, ID, shouldThrow) VALUES ('a', 1, false);
INSERT INTO `TEST` (K, ID, shouldThrow) VALUES ('b', 2, true);
INSERT INTO `TEST` (K, ID, shouldThrow) VALUES ('c', 3, false);
ASSERT VALUES `REPARTITIONED` (KSQL_COL_0, ID) VALUES (0, 1);
ASSERT VALUES `REPARTITIONED` (ID) VALUES (2);
ASSERT VALUES `REPARTITIONED` (KSQL_COL_0, ID) VALUES (0, 3);

--@test: partition-by - non-KAFKA key format
CREATE STREAM INPUT (ID INT KEY, VAL DOUBLE) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select * from INPUT partition by VAL;
INSERT INTO `INPUT` (ID, VAL) VALUES (10, 10.02);
ASSERT VALUES `OUTPUT` (VAL, ID) VALUES (10.02, 10);

--@test: partition-by - partition by array key
CREATE STREAM INPUT (ID INT KEY, VAL ARRAY<INT>) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select * from INPUT partition by VAL;
INSERT INTO `INPUT` (ID, VAL) VALUES (10, ARRAY[12, 1]);
ASSERT VALUES `OUTPUT` (VAL, ID) VALUES (ARRAY[12, 1], 10);

--@test: partition-by - partition by struct key
CREATE STREAM INPUT (ID INT KEY, VAL STRUCT<F1 INT, F2 INT>) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select * from INPUT partition by VAL;
INSERT INTO `INPUT` (ID, VAL) VALUES (10, STRUCT(F1:=1, F2:=2));
ASSERT VALUES `OUTPUT` (VAL, ID) VALUES (STRUCT(F1:=1, F2:=2), 10);

--@test: partition-by - partition by struct key when original is unwrapped
CREATE STREAM INPUT (ID INT KEY, VAL STRUCT<F1 INT, F2 INT>) with (kafka_topic='input', format='JSON', wrap_single_value=false);
CREATE STREAM OUTPUT AS select * from INPUT partition by VAL;
INSERT INTO `INPUT` (ID, VAL) VALUES (10, STRUCT(F1:=1, F2:=2));
ASSERT VALUES `OUTPUT` (VAL, ID) VALUES (STRUCT(F1:=1, F2:=2), 10);

--@test: partition-by - partition by field within struct key
CREATE STREAM INPUT (ID STRUCT<F1 INT, F2 INT> KEY, VAL INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select * from INPUT partition by ID->F1;
INSERT INTO `INPUT` (ID, VAL) VALUES (STRUCT(F1:=1, F2:=2), 10);
ASSERT VALUES `OUTPUT` (F1, ID, VAL) VALUES (1, STRUCT(F1:=1, F2:=2), 10);

--@test: partition-by - partition by field within struct key don't select struct key
CREATE STREAM INPUT (ID STRUCT<F1 INT, F2 INT> KEY, VAL INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select ID->F1 AS F1, ID->F2 as F2, VAL from INPUT partition by ID->F1;
INSERT INTO `INPUT` (ID, VAL) VALUES (STRUCT(F1:=1, F2:=2), 10);
ASSERT VALUES `OUTPUT` (F1, F2, VAL) VALUES (1, 2, 10);

--@test: partition-by - partition by create struct
CREATE STREAM INPUT (ID STRUCT<F1 INT, F2 INT> KEY, VAL INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select * from INPUT partition by STRUCT(a:=id->f1, b:=val);
INSERT INTO `INPUT` (ID, VAL) VALUES (STRUCT(F1:=1, F2:=2), 10);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, ID, VAL) VALUES (STRUCT(A:=1, B:=10), STRUCT(F1:=1, F2:=2), 10);

--@test: partition-by - partition by map key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Map keys, including types that contain maps, are not supported as they may lead to unexpected behavior due to inconsistent serialization. Key column name: `VAL`. Column type: MAP<STRING, INTEGER>. See https://github.com/confluentinc/ksql/issues/6621 for more.
CREATE STREAM INPUT (ID INT KEY, VAL MAP<STRING, INT>) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select * from INPUT partition by VAL;
--@test: partition-by - partition by nested map key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Map keys, including types that contain maps, are not supported as they may lead to unexpected behavior due to inconsistent serialization. Key column name: `VAL`. Column type: STRUCT<`F1` MAP<STRING, INTEGER>>. See https://github.com/confluentinc/ksql/issues/6621 for more.
CREATE STREAM INPUT (ID INT KEY, VAL STRUCT<F1 MAP<STRING, INT>>) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select * from INPUT partition by VAL;

--@test: partition-by - multiple columns - select star
CREATE STREAM INPUT (NAME STRING, ID INT, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select * from INPUT partition by ID, AGE;
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', 10, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, NULL);
INSERT INTO `INPUT` (NAME) VALUES ('bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (10, 30, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (NULL, 30, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (NULL, NULL, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE) VALUES (NULL, NULL);
ASSERT stream OUTPUT (ID INT KEY, AGE INT KEY, NAME STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - multiple columns - select star - reorder columns
CREATE STREAM INPUT (NAME STRING, ID INT, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select * from INPUT partition by AGE, ID;
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', 10, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, NULL);
INSERT INTO `INPUT` (NAME) VALUES ('bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (10, 30, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (NULL, 30, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (NULL, NULL, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE) VALUES (NULL, NULL);
ASSERT stream OUTPUT (AGE INT KEY, ID INT KEY, NAME STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - multiple columns - select explicit
CREATE STREAM INPUT (NAME STRING, ID INT, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select ID, AGE, NAME from INPUT partition by ID, AGE;
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', 10, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, NULL);
INSERT INTO `INPUT` (NAME) VALUES ('bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (10, 30, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (NULL, 30, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (NULL, NULL, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE) VALUES (NULL, NULL);
ASSERT stream OUTPUT (ID INT KEY, AGE INT KEY, NAME STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - multiple columns - select explicit - reorder 
CREATE STREAM INPUT (NAME STRING, ID INT, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS SELECT age, id, name FROM input PARTITION BY age, id;
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', 10, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, NULL);
INSERT INTO `INPUT` (NAME) VALUES ('bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (10, 30, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (NULL, 30, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (NULL, NULL, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE) VALUES (NULL, NULL);
ASSERT stream OUTPUT (AGE INT KEY, ID INT KEY, NAME STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - multiple columns - select explicit - reorder partition by but keep same order in project
CREATE STREAM INPUT (NAME STRING, ID INT, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS SELECT id, age, name FROM input PARTITION BY age, id;
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', 10, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, NULL);
INSERT INTO `INPUT` (NAME) VALUES ('bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (10, 30, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (NULL, 30, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (NULL, NULL, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE) VALUES (NULL, NULL);
ASSERT stream OUTPUT (AGE INT KEY, ID INT KEY, NAME STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - multiple columns - some key some value
CREATE STREAM INPUT (NAME STRING KEY, ID INT, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS SELECT id, age, name FROM input PARTITION BY id, name;
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', 10, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, 30);
INSERT INTO `INPUT` (ID, AGE) VALUES (10, 30);
INSERT INTO `INPUT` (NAME) VALUES ('bob');
INSERT INTO `INPUT` (NAME) VALUES ('bob');
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (10, 'bob', 30);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (NULL, 'bob', 30);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (10, NULL, 30);
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (NULL, 'bob');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (NULL, NULL);
ASSERT stream OUTPUT (ID INT KEY, NAME STRING KEY, AGE INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - multiple columns - select * - some key some value
CREATE STREAM INPUT (NAME STRING KEY, ID INT, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM input PARTITION BY id, name;
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', 10, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, 30);
INSERT INTO `INPUT` (ID, AGE) VALUES (10, 30);
INSERT INTO `INPUT` (NAME) VALUES ('bob');
INSERT INTO `INPUT` (NAME) VALUES ('bob');
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (10, 'bob', 30);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (NULL, 'bob', 30);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (10, NULL, 30);
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (NULL, 'bob');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (NULL, NULL);
ASSERT stream OUTPUT (ID INT KEY, NAME STRING KEY, AGE INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - multiple columns - some key some value - key first
CREATE STREAM INPUT (NAME STRING KEY, ID INT, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select ID, AGE, NAME from INPUT partition by NAME, ID;
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', 10, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, 30);
INSERT INTO `INPUT` (ID, AGE) VALUES (10, 30);
INSERT INTO `INPUT` (NAME) VALUES ('bob');
INSERT INTO `INPUT` (AGE) VALUES (5);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (10, 'bob', 30);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (NULL, 'bob', 30);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (10, NULL, 30);
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (NULL, 'bob');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (NULL, NULL);
ASSERT stream OUTPUT (NAME STRING KEY, ID INT KEY, AGE INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - multiple columns - select * - some key some value - key first
CREATE STREAM INPUT (NAME STRING KEY, ID INT, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM input PARTITION BY name, id;
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', 10, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, 30);
INSERT INTO `INPUT` (ID, AGE) VALUES (10, 30);
INSERT INTO `INPUT` (NAME) VALUES ('bob');
INSERT INTO `INPUT` (AGE) VALUES (5);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (10, 'bob', 30);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (NULL, 'bob', 30);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (10, NULL, 30);
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (NULL, 'bob');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (NULL, NULL);
ASSERT stream OUTPUT (NAME STRING KEY, ID INT KEY, AGE INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - multiple columns - some key some value - properly ordered in selection
CREATE STREAM INPUT (NAME STRING KEY, ID INT, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS SELECT name, age, id FROM input PARTITION BY name, id;
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', 10, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, 30);
INSERT INTO `INPUT` (ID, AGE) VALUES (10, 30);
INSERT INTO `INPUT` (NAME) VALUES ('bob');
INSERT INTO `INPUT` (AGE) VALUES (5);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (10, 'bob', 30);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (NULL, 'bob', 30);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (10, NULL, 30);
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (NULL, 'bob');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (NULL, NULL);
ASSERT stream OUTPUT (NAME STRING KEY, ID INT KEY, AGE INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - multiple key columns
CREATE STREAM INPUT (NAME STRING KEY, ID INT KEY, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select ID, AGE, NAME from INPUT partition by ID, NAME;
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', 10, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES (NULL, 10, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES (NULL, NULL, 30);
INSERT INTO `INPUT` (AGE) VALUES (30);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (10, 'bob', 30);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (10, NULL, 30);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (NULL, NULL, 30);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (NULL, NULL, 30);
ASSERT stream OUTPUT (ID INT KEY, NAME STRING KEY, AGE INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - multiple key columns - reordered
CREATE STREAM INPUT (NAME STRING KEY, ID INT KEY, AGE INT) with (kafka_topic='input', format='DELIMITED');
CREATE STREAM OUTPUT AS select ID, AGE, NAME from INPUT partition by ID, NAME;
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', 10, 30);
ASSERT VALUES `OUTPUT` (ID, NAME, AGE) VALUES (10, 'bob', 30);
ASSERT stream OUTPUT (ID INT KEY, NAME STRING KEY, AGE INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - multiple columns with expressions
CREATE STREAM INPUT (AGE INT KEY, NAME STRING, ID INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select ABS(ID), 1, AGE + 2 AS K3, AGE, NAME from INPUT partition by ABS(ID), 1, AGE + 2;
INSERT INTO `INPUT` (NAME, ID) VALUES ('bob', 10);
INSERT INTO `INPUT` (AGE, NAME, ID) VALUES (30, 'bob', NULL);
INSERT INTO `INPUT` (AGE) VALUES (30);
INSERT INTO `INPUT` (NAME) VALUES ('bob');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1, K3, AGE, NAME) VALUES (10, 1, NULL, NULL, 'bob');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1, K3, AGE, NAME) VALUES (NULL, 1, 32, 30, 'bob');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1, K3) VALUES (NULL, 1, 32);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1, K3) VALUES (NULL, 1, NULL);
ASSERT stream OUTPUT (KSQL_COL_0 INT KEY, KSQL_COL_1 INT KEY, K3 INT KEY, AGE INT, NAME STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - multiple columns including ROWTIME
CREATE STREAM INPUT (K STRING KEY, ID bigint) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select * from INPUT partition by ID, ROWTIME;
INSERT INTO `INPUT` (ID, ROWTIME) VALUES (22, 10);
INSERT INTO `INPUT` (ID, ROWTIME) VALUES (NULL, 11);
INSERT INTO `INPUT` (ROWTIME) VALUES (12);
ASSERT VALUES `OUTPUT` (ID, ROWTIME, K, ROWTIME) VALUES (22, 10, NULL, 10);
ASSERT VALUES `OUTPUT` (ID, ROWTIME, K, ROWTIME) VALUES (NULL, 11, NULL, 11);
ASSERT VALUES `OUTPUT` (ID, ROWTIME, ROWTIME) VALUES (NULL, NULL, 12);

--@test: partition-by - multiple columns including null
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Cannot PARTITION BY multiple columns including NULL
CREATE STREAM INPUT (NAME STRING, ID INT, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select ID, AGE, NAME from INPUT partition by NULL, ID;
--@test: partition-by - multiple columns - missing key from projection
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the partitioning expressions INPUT.ID and INPUT.AGE in its projection (eg, SELECT INPUT.ID, INPUT.AGE...).
CREATE STREAM INPUT (NAME STRING, ID INT, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select ID, NAME from INPUT partition by ID, AGE;
--@test: partition-by - multiple columns - KAFKA key format
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Key format does not support schema.
format: KAFKA
schema: Persistence{columns=[`ID` INTEGER KEY, `AGE` INTEGER KEY], features=[]}
reason: The 'KAFKA' format only supports a single field. Got: [`ID` INTEGER KEY, `AGE` INTEGER KEY]
CREATE STREAM INPUT (NAME STRING, ID INT, AGE INT) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS select ID, AGE, NAME from INPUT partition by ID, AGE;

--@test: partition-by - multiple columns - KAFKA to non-KAFKA key format
CREATE STREAM INPUT (NAME STRING, ID INT, AGE INT) with (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT WITH (KEY_FORMAT='JSON') AS select * from INPUT partition by ID, AGE;
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', 10, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, 30);
INSERT INTO `INPUT` (NAME, ID, AGE) VALUES ('bob', NULL, NULL);
INSERT INTO `INPUT` (NAME) VALUES ('bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (10, 30, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (NULL, 30, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE, NAME) VALUES (NULL, NULL, 'bob');
ASSERT VALUES `OUTPUT` (ID, AGE) VALUES (NULL, NULL);
ASSERT stream OUTPUT (ID INT KEY, AGE INT KEY, NAME STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: partition-by - multiple columns - including map
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Map keys, including types that contain maps, are not supported as they may lead to unexpected behavior due to inconsistent serialization. Key column name: `ID`. Column type: MAP<STRING, INTEGER>. See https://github.com/confluentinc/ksql/issues/6621 for more.
CREATE STREAM INPUT (NAME STRING, ID MAP<STRING, INT>, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select ID, AGE, NAME from INPUT partition by ID, AGE;
--@test: partition-by - partition by nothing
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: PARTITION BY requires at least one expression
CREATE STREAM INPUT (NAME STRING, ID INT, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select ID, AGE, NAME from INPUT partition by ();
--@test: partition-by - partition by output col should give good error message
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: PARTITION BY column 'SOMETHING' cannot be resolved. 'SOMETHING' must be a column in the source schema since PARTITION BY is applied on the input.
CREATE STREAM INPUT (NAME STRING, ID INT, AGE INT) with (kafka_topic='input', format='JSON');
CREATE STREAM OUTPUT AS select ID, AGE, NAME AS something from INPUT partition by something;
