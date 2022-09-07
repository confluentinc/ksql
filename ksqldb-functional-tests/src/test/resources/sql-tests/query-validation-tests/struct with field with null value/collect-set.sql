--@test: collect-set - collect_set with arrays, structs, and maps - AVRO
CREATE STREAM INPUT (ID BIGINT KEY, F0 ARRAY<STRUCT<A VARCHAR, M MAP<STRING, DOUBLE>, D DECIMAL(4,1)>>) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE OUTPUT AS SELECT ID, collect_set(F0) AS COLLECTED FROM INPUT GROUP BY ID;
INSERT INTO `INPUT` (ID, F0) VALUES (0, ARRAY[STRUCT(A:='Early0', M:=MAP('Early0':=1.234), D:=123.4), STRUCT(A:='Early2', M:=MAP('Early2':=1.23456))]);
INSERT INTO `INPUT` (ID, F0) VALUES (0, ARRAY[STRUCT(A:='Early1', M:=MAP('Early0':=2.345), D:=234.5)]);
INSERT INTO `INPUT` (ID, F0) VALUES (1, ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=NULL)]);
INSERT INTO `INPUT` (ID, F0) VALUES (1, ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=0.1)]);
INSERT INTO `INPUT` (ID, F0) VALUES (1, ARRAY[NULL]);
INSERT INTO `INPUT` (ID, F0) VALUES (1, NULL);
ASSERT VALUES `OUTPUT` (ID, COLLECTED) VALUES (0, ARRAY[ARRAY[STRUCT(A:='Early0', M:=MAP('Early0':=1.234), D:=123.4), STRUCT(A:='Early2', M:=MAP('Early2':=1.23456), D:=NULL)]]);
ASSERT VALUES `OUTPUT` (ID, COLLECTED) VALUES (0, ARRAY[ARRAY[STRUCT(A:='Early0', M:=MAP('Early0':=1.234), D:=123.4), STRUCT(A:='Early2', M:=MAP('Early2':=1.23456), D:=NULL)], ARRAY[STRUCT(A:='Early1', M:=MAP('Early0':=2.345), D:=234.5)]]);
ASSERT VALUES `OUTPUT` (ID, COLLECTED) VALUES (1, ARRAY[ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=NULL)]]);
ASSERT VALUES `OUTPUT` (ID, COLLECTED) VALUES (1, ARRAY[ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=NULL)], ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=0.1)]]);
ASSERT VALUES `OUTPUT` (ID, COLLECTED) VALUES (1, ARRAY[ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=NULL)], ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=0.1)], ARRAY[NULL]]);
ASSERT VALUES `OUTPUT` (ID, COLLECTED) VALUES (1, ARRAY[ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=NULL)], ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=0.1)], ARRAY[NULL], NULL]);

--@test: collect-set - collect_set with arrays, structs, and maps - JSON
CREATE STREAM INPUT (ID BIGINT KEY, F0 ARRAY<STRUCT<A VARCHAR, M MAP<STRING, DOUBLE>, D DECIMAL(4,1)>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT ID, collect_set(F0) AS COLLECTED FROM INPUT GROUP BY ID;
INSERT INTO `INPUT` (ID, F0) VALUES (0, ARRAY[STRUCT(A:='Early0', M:=MAP('Early0':=1.234), D:=123.4), STRUCT(A:='Early2', M:=MAP('Early2':=1.23456))]);
INSERT INTO `INPUT` (ID, F0) VALUES (0, ARRAY[STRUCT(A:='Early1', M:=MAP('Early0':=2.345), D:=234.5)]);
INSERT INTO `INPUT` (ID, F0) VALUES (1, ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=NULL)]);
INSERT INTO `INPUT` (ID, F0) VALUES (1, ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=0.1)]);
INSERT INTO `INPUT` (ID, F0) VALUES (1, ARRAY[NULL]);
INSERT INTO `INPUT` (ID, F0) VALUES (1, NULL);
ASSERT VALUES `OUTPUT` (ID, COLLECTED) VALUES (0, ARRAY[ARRAY[STRUCT(A:='Early0', M:=MAP('Early0':=1.234), D:=123.4), STRUCT(A:='Early2', M:=MAP('Early2':=1.23456), D:=NULL)]]);
ASSERT VALUES `OUTPUT` (ID, COLLECTED) VALUES (0, ARRAY[ARRAY[STRUCT(A:='Early0', M:=MAP('Early0':=1.234), D:=123.4), STRUCT(A:='Early2', M:=MAP('Early2':=1.23456), D:=NULL)], ARRAY[STRUCT(A:='Early1', M:=MAP('Early0':=2.345), D:=234.5)]]);
ASSERT VALUES `OUTPUT` (ID, COLLECTED) VALUES (1, ARRAY[ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=NULL)]]);
ASSERT VALUES `OUTPUT` (ID, COLLECTED) VALUES (1, ARRAY[ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=NULL)], ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=0.1)]]);
ASSERT VALUES `OUTPUT` (ID, COLLECTED) VALUES (1, ARRAY[ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=NULL)], ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=0.1)], ARRAY[NULL]]);
ASSERT VALUES `OUTPUT` (ID, COLLECTED) VALUES (1, ARRAY[ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=NULL)], ARRAY[STRUCT(A:='Later0', M:=MAP('Early0':=3.45), D:=0.1)], ARRAY[NULL], NULL]);

--@test: collect-set - collect_set maps - AVRO
CREATE STREAM TEST (ID BIGINT KEY, VALUE MAP<VARCHAR, BIGINT>) WITH (kafka_topic='test_topic',value_format='AVRO');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, MAP('Record0':=1));
INSERT INTO `TEST` (ID, VALUE) VALUES (0, MAP('Record1':=100));
INSERT INTO `TEST` (ID, VALUE) VALUES (100, MAP('Record0':=100, 'Record2':=2));
INSERT INTO `TEST` (ID, VALUE) VALUES (100, MAP('Record2':=2, 'Record0':=100));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[MAP('Record0':=1)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[MAP('Record0':=1), MAP('Record1':=100)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[MAP('Record0':=100, 'Record2':=2)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[MAP('Record0':=100, 'Record2':=2)]);

--@test: collect-set - collect_set maps - JSON
CREATE STREAM TEST (ID BIGINT KEY, VALUE MAP<VARCHAR, BIGINT>) WITH (kafka_topic='test_topic',value_format='JSON');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, MAP('Record0':=1));
INSERT INTO `TEST` (ID, VALUE) VALUES (0, MAP('Record1':=100));
INSERT INTO `TEST` (ID, VALUE) VALUES (100, MAP('Record0':=100, 'Record2':=2));
INSERT INTO `TEST` (ID, VALUE) VALUES (100, MAP('Record2':=2, 'Record0':=100));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[MAP('Record0':=1)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[MAP('Record0':=1), MAP('Record1':=100)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[MAP('Record0':=100, 'Record2':=2)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[MAP('Record0':=100, 'Record2':=2)]);

--@test: collect-set - collect_set arrays - AVRO
CREATE STREAM TEST (ID BIGINT KEY, VALUE ARRAY<INT>) WITH (kafka_topic='test_topic',value_format='AVRO');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, ARRAY[0, 0, 1, 0, -1]);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, ARRAY[1, 2, 3, 4, -1]);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, ARRAY[0, 0, 1, 0, -1]);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, ARRAY[0, 0, 1, 0, -1]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[ARRAY[0, 0, 1, 0, -1]]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[ARRAY[0, 0, 1, 0, -1], ARRAY[1, 2, 3, 4, -1]]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[ARRAY[0, 0, 1, 0, -1]]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[ARRAY[0, 0, 1, 0, -1]]);

--@test: collect-set - collect_set arrays - JSON
CREATE STREAM TEST (ID BIGINT KEY, VALUE ARRAY<INT>) WITH (kafka_topic='test_topic',value_format='JSON');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, ARRAY[0, 0, 1, 0, -1]);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, ARRAY[1, 2, 3, 4, -1]);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, ARRAY[0, 0, 1, 0, -1]);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, ARRAY[0, 0, 1, 0, -1]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[ARRAY[0, 0, 1, 0, -1]]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[ARRAY[0, 0, 1, 0, -1], ARRAY[1, 2, 3, 4, -1]]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[ARRAY[0, 0, 1, 0, -1]]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[ARRAY[0, 0, 1, 0, -1]]);

--@test: collect-set - collect_set struct - AVRO
CREATE STREAM TEST (ID BIGINT KEY, VALUE STRUCT<A VARCHAR, B BIGINT>) WITH (kafka_topic='test_topic',value_format='AVRO');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, STRUCT(A:='Record0', B:=1));
INSERT INTO `TEST` (ID, VALUE) VALUES (0, STRUCT(A:='Record1', B:=100));
INSERT INTO `TEST` (ID, VALUE) VALUES (100, STRUCT(A:='Record0', B:=100));
INSERT INTO `TEST` (ID, VALUE) VALUES (100, STRUCT(B:=100, A:='Record0'));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[STRUCT(A:='Record0', B:=1)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[STRUCT(A:='Record0', B:=1), STRUCT(A:='Record1', B:=100)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[STRUCT(A:='Record0', B:=100)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[STRUCT(A:='Record0', B:=100)]);

--@test: collect-set - collect_set struct - JSON
CREATE STREAM TEST (ID BIGINT KEY, VALUE STRUCT<A VARCHAR, B BIGINT>) WITH (kafka_topic='test_topic',value_format='JSON');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, STRUCT(A:='Record0', B:=1));
INSERT INTO `TEST` (ID, VALUE) VALUES (0, STRUCT(A:='Record1', B:=100));
INSERT INTO `TEST` (ID, VALUE) VALUES (100, STRUCT(A:='Record0', B:=100));
INSERT INTO `TEST` (ID, VALUE) VALUES (100, STRUCT(B:=100, A:='Record0'));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[STRUCT(A:='Record0', B:=1)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[STRUCT(A:='Record0', B:=1), STRUCT(A:='Record1', B:=100)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[STRUCT(A:='Record0', B:=100)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[STRUCT(A:='Record0', B:=100)]);

--@test: collect-set - collect_set struct - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, VALUE STRUCT<A VARCHAR, B BIGINT>) WITH (kafka_topic='test_topic',value_format='PROTOBUF');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, STRUCT(A:='Record0', B:=1));
INSERT INTO `TEST` (ID, VALUE) VALUES (0, STRUCT(A:='Record1', B:=100));
INSERT INTO `TEST` (ID, VALUE) VALUES (100, STRUCT(A:='Record0', B:=100));
INSERT INTO `TEST` (ID, VALUE) VALUES (100, STRUCT(B:=100, A:='Record0'));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[STRUCT(A:='Record0', B:=1)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[STRUCT(A:='Record0', B:=1), STRUCT(A:='Record1', B:=100)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[STRUCT(A:='Record0', B:=100)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[STRUCT(A:='Record0', B:=100)]);

--@test: collect-set - collect_set struct - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, VALUE STRUCT<A VARCHAR, B BIGINT>) WITH (kafka_topic='test_topic',value_format='PROTOBUF_NOSR');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, STRUCT(A:='Record0', B:=1));
INSERT INTO `TEST` (ID, VALUE) VALUES (0, STRUCT(A:='Record1', B:=100));
INSERT INTO `TEST` (ID, VALUE) VALUES (100, STRUCT(A:='Record0', B:=100));
INSERT INTO `TEST` (ID, VALUE) VALUES (100, STRUCT(B:=100, A:='Record0'));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[STRUCT(A:='Record0', B:=1)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[STRUCT(A:='Record0', B:=1), STRUCT(A:='Record1', B:=100)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[STRUCT(A:='Record0', B:=100)]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[STRUCT(A:='Record0', B:=100)]);

--@test: collect-set - collect_set int - AVRO
CREATE STREAM TEST (ID BIGINT KEY, VALUE integer) WITH (kafka_topic='test_topic',value_format='AVRO');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 0);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 100);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 500);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 100);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[0]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[0, 100]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500, 100]);

--@test: collect-set - collect_set int - JSON
CREATE STREAM TEST (ID BIGINT KEY, VALUE integer) WITH (kafka_topic='test_topic',value_format='JSON');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 0);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 100);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 500);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 100);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[0]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[0, 100]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500, 100]);

--@test: collect-set - collect_set int - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, VALUE integer) WITH (kafka_topic='test_topic',value_format='PROTOBUF');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 0);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 100);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 500);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 100);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[0]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[0, 100]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500, 100]);

--@test: collect-set - collect_set int - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, VALUE integer) WITH (kafka_topic='test_topic',value_format='PROTOBUF_NOSR');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 0);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 100);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 500);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 100);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[0]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[0, 100]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500, 100]);

--@test: collect-set - collect_set long - AVRO
CREATE STREAM TEST (ID BIGINT KEY, VALUE bigint) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 2147483648);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 500);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 100);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 100);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[2147483648]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500, 100]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[2147483648, 100]);

--@test: collect-set - collect_set long - JSON
CREATE STREAM TEST (ID BIGINT KEY, VALUE bigint) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 2147483648);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 500);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 100);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 100);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[2147483648]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500, 100]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[2147483648, 100]);

--@test: collect-set - collect_set long - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, VALUE bigint) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 2147483648);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 500);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 100);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 100);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[2147483648]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500, 100]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[2147483648, 100]);

--@test: collect-set - collect_set long - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, VALUE bigint) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 2147483648);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 500);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 100);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 100);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[2147483648]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500, 100]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[2147483648, 100]);

--@test: collect-set - collect_set decimal - AVRO
CREATE STREAM TEST (ID BIGINT KEY, VALUE DECIMAL(4,1)) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 5.4);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 100.1);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 500.9);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 300.8);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, NULL);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 300.8);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, NULL);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[5.4]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[5.4, 100.1]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9, 300.8]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9, 300.8, NULL]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9, 300.8, NULL]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9, 300.8, NULL]);

--@test: collect-set - collect_set decimal - JSON
CREATE STREAM TEST (ID BIGINT KEY, VALUE DECIMAL(4,1)) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 5.4);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 100.1);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 500.9);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 300.8);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, NULL);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 300.8);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, NULL);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[5.4]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[5.4, 100.1]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9, 300.8]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9, 300.8, NULL]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9, 300.8, NULL]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9, 300.8, NULL]);

--@test: collect-set - collect_set double - AVRO
CREATE STREAM TEST (ID BIGINT KEY, VALUE double) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 5.4);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 100.1);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 500.9);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 300.8);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[5.4]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[5.4, 100.1]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9, 300.8]);

--@test: collect-set - collect_set double - JSON
CREATE STREAM TEST (ID BIGINT KEY, VALUE double) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 5.4);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 100.1);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 500.9);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 300.8);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[5.4]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[5.4, 100.1]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9, 300.8]);

--@test: collect-set - collect_set double - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, VALUE double) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 5.4);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 100.1);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 500.9);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 300.8);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[5.4]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[5.4, 100.1]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9, 300.8]);

--@test: collect-set - collect_set double - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, VALUE double) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 5.4);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 100.1);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 500.9);
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 300.8);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[5.4]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[5.4, 100.1]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY[500.9, 300.8]);

--@test: collect-set - collect_set string - AVRO
CREATE STREAM TEST (ID BIGINT KEY, VALUE varchar) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'foo');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'bar');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'foo');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, NULL);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['foo']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['foo', 'bar']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz', 'foo']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz', 'foo', NULL]);

--@test: collect-set - collect_set string - JSON
CREATE STREAM TEST (ID BIGINT KEY, VALUE varchar) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'foo');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'bar');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'foo');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, NULL);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['foo']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['foo', 'bar']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz', 'foo']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz', 'foo', NULL]);

--@test: collect-set - collect_set string - PROTOBUFs - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, VALUE varchar) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'foo');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'bar');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'foo');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, NULL);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['foo']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['foo', 'bar']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz', 'foo']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz', 'foo', '']);

--@test: collect-set - collect_set string - PROTOBUFs - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, VALUE varchar) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'foo');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 'bar');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'baz');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, 'foo');
INSERT INTO `TEST` (ID, VALUE) VALUES (100, NULL);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['foo']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['foo', 'bar']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz', 'foo']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (100, ARRAY['baz', 'foo', '']);

--@test: collect-set - collect_set bool map - JSON
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, boolean>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':=true, 'key2':=false));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':=false, 'key2':=true));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':=true, 'key2':=true));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[true]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[true, false]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[true, false]);

--@test: collect-set - collect_set bool map - AVRO
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, boolean>) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':=true, 'key2':=false));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':=false, 'key2':=true));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':=true, 'key2':=true));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[true]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[true, false]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[true, false]);

--@test: collect-set - collect_set bool map - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, boolean>) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':=true, 'key2':=false));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':=false, 'key2':=true));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':=true, 'key2':=true));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[true]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[true, false]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[true, false]);

--@test: collect-set - collect_set bool map - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, boolean>) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':=true, 'key2':=false));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':=false, 'key2':=true));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':=true, 'key2':=true));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[true]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[true, false]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[true, false]);

--@test: collect-set - collect_list timestamp map - JSON
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, timestamp>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-01T00:00:00.010', 'key2':='1970-01-01T00:00:00.015'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-01T00:00:00.020', 'key2':='1970-01-01T00:00:00.025'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-01T00:00:00.020', 'key2':='1970-01-01T00:00:00.035'));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-01T00:00:00.010']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-01T00:00:00.010', '1970-01-01T00:00:00.020']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-01T00:00:00.010', '1970-01-01T00:00:00.020']);

--@test: collect-set - collect_list timestamp map - AVRO
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, timestamp>) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-01T00:00:00.010', 'key2':='1970-01-01T00:00:00.015'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-01T00:00:00.020', 'key2':='1970-01-01T00:00:00.025'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-01T00:00:00.020', 'key2':='1970-01-01T00:00:00.035'));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-01T00:00:00.010']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-01T00:00:00.010', '1970-01-01T00:00:00.020']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-01T00:00:00.010', '1970-01-01T00:00:00.020']);

--@test: collect-set - collect_list timestamp map - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, timestamp>) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-01T00:00:00.010', 'key2':='1970-01-01T00:00:00.015'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-01T00:00:00.020', 'key2':='1970-01-01T00:00:00.025'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-01T00:00:00.020', 'key2':='1970-01-01T00:00:00.035'));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-01T00:00:00.010']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-01T00:00:00.010', '1970-01-01T00:00:00.020']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-01T00:00:00.010', '1970-01-01T00:00:00.020']);

--@test: collect-set - collect_list timestamp map - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, timestamp>) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-01T00:00:00.010', 'key2':='1970-01-01T00:00:00.015'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-01T00:00:00.020', 'key2':='1970-01-01T00:00:00.025'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-01T00:00:00.020', 'key2':='1970-01-01T00:00:00.035'));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-01T00:00:00.010']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-01T00:00:00.010', '1970-01-01T00:00:00.020']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-01T00:00:00.010', '1970-01-01T00:00:00.020']);

--@test: collect-set - collect_list time map - JSON
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, time>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='00:00:10', 'key2':='00:00'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='00:00:20', 'key2':='00:00'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='00:00:20', 'key2':='00:00'));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['00:00:10']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['00:00:10', '00:00:20']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['00:00:10', '00:00:20']);

--@test: collect-set - collect_list time map - AVRO
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, time>) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='00:00:10', 'key2':='00:00'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='00:00:20', 'key2':='00:00'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='00:00:20', 'key2':='00:00'));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['00:00:10']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['00:00:10', '00:00:20']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['00:00:10', '00:00:20']);

--@test: collect-set - collect_list time map - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, time>) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='00:00:10', 'key2':='00:00'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='00:00:20', 'key2':='00:00'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='00:00:20', 'key2':='00:00'));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['00:00:10']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['00:00:10', '00:00:20']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['00:00:10', '00:00:20']);

--@test: collect-set - collect_list time map - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, time>) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='00:00:10', 'key2':='00:00'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='00:00:20', 'key2':='00:00'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='00:00:20', 'key2':='00:00'));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['00:00:10']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['00:00:10', '00:00:20']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['00:00:10', '00:00:20']);

--@test: collect-set - collect_list date map - JSON
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, date>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-11', 'key2':='1970-01-16'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-21', 'key2':='1970-01-26'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-21', 'key2':='1970-02-05'));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-11']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-11', '1970-01-21']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-11', '1970-01-21']);

--@test: collect-set - collect_list date map - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, date>) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-11', 'key2':='1970-01-16'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-21', 'key2':='1970-01-26'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-21', 'key2':='1970-02-05'));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-11']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-11', '1970-01-21']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-11', '1970-01-21']);

--@test: collect-set - collect_list date map - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, date>) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-11', 'key2':='1970-01-16'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-21', 'key2':='1970-01-26'));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='1970-01-21', 'key2':='1970-02-05'));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-11']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-11', '1970-01-21']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['1970-01-11', '1970-01-21']);

--@test: collect-set - collect_list bytes map - JSON
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE map<varchar, bytes>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, collect_set(value['key1']) AS collected FROM test group by id;
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='YQ=='));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='Yg=='));
INSERT INTO `TEST` (ID, name, value) VALUES (0, 'zero', MAP('key1':='Yg=='));
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['YQ==']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['YQ==', 'Yg==']);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY['YQ==', 'Yg==']);

--@test: collect-set - collect_set with limit of 1
SET 'ksql.functions.collect_set.limit' = '1';CREATE STREAM TEST (ID BIGINT KEY, VALUE integer) WITH (kafka_topic='test_topic',value_format='JSON');
CREATE TABLE S2 as SELECT ID, collect_set(value) as collected FROM test group by id;
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 0);
INSERT INTO `TEST` (ID, VALUE) VALUES (0, 100);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[0]);
ASSERT VALUES `S2` (ID, COLLECTED) VALUES (0, ARRAY[0]);

