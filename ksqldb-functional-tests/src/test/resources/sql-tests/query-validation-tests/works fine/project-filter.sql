--@test: project-filter - project and filter
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE double) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM S1 as SELECT K, name FROM test where id > 100;
INSERT INTO `TEST` (K, ID, NAME, VALUE) VALUES ('0', 0, 'zero', 0.0);
INSERT INTO `TEST` (K, ID, NAME, VALUE) VALUES ('100', 100, '100', 0.0);
INSERT INTO `TEST` (K, ID, NAME, VALUE) VALUES ('101', 101, '101', 0.0);
ASSERT VALUES `S1` (K, NAME) VALUES ('101', '101');

--@test: project-filter - project string with embedded code
CREATE STREAM TEST (K STRING KEY, ID bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM S1 as SELECT K, '" + new java.util.function.Supplier<String>(){public String get() {return "boom";}}.get() + "' as x  FROM test;
INSERT INTO `TEST` (K, ID) VALUES ('0', 0);
ASSERT VALUES `S1` (K, X) VALUES ('0', '" + new java.util.function.Supplier<String>(){public String get() {return "boom";}}.get() + "');

--@test: project-filter - Json Map filter
CREATE STREAM TEST (K STRING KEY, ID bigint, THING MAP<VARCHAR, VARCHAR>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM S1 as SELECT K, ID FROM TEST WHERE THING['status']='false';
INSERT INTO `TEST` (K, id, thing) VALUES ('0', 1, MAP('other':='11', 'status':='false'));
INSERT INTO `TEST` (K, id, thing) VALUES ('0', 2, MAP('other':='12', 'status':='true'));
INSERT INTO `TEST` (K, id, thing) VALUES ('0', 3, MAP('other':='13', 'status':='true'));
INSERT INTO `TEST` (K, id, thing) VALUES ('0', 4, MAP('other':='13', 'status':='false'));
ASSERT VALUES `S1` (K, ID) VALUES ('0', 1);
ASSERT VALUES `S1` (K, ID) VALUES ('0', 4);

--@test: project-filter - WHERE with many comparisons. This tests the fix for #1784
CREATE STREAM events (K STRING KEY, id int, field0 varchar) WITH (KAFKA_TOPIC='events', VALUE_FORMAT='json');
CREATE STREAM eventstest AS SELECT K, id, 'x_0' AS field1, field0 FROM events WHERE ((id=1 OR id=2 OR id=3 OR id=4) AND (field0='0x10' OR field0='0x11' OR field0='0x12' OR field0='0x13' OR field0='0x14' OR field0='0x15' OR field0='0x16' OR field0='0x17' OR field0='0x18' OR field0='0x19' OR field0='0x1A' OR field0='0x1B' OR field0='0x1C' OR field0='0x1D' OR field0='0x1E' OR field0='0x1F' OR field0='0x20' OR field0='0x21' OR field0='0x22' OR field0='0x23' OR field0='0x24' OR field0='0x25' OR field0='0x26' OR field0='0x27' OR field0='0x28' OR field0='0x29' OR field0='0x2A' OR field0='0x2B' OR field0='0x2C' OR field0='0x2D' OR field0='0x2E' OR field0='0x2F' OR field0='0x30' OR field0='0x31' OR field0='0x32' OR field0='0x33' OR field0='0x34' OR field0='0x35' OR field0='0x36' OR field0='0x37' OR field0='0x38' OR field0='0x39' OR field0='0x3A' OR field0='0x3B' OR field0='0x3C' OR field0='0x3D' OR field0='0x3E' OR field0='0x3F'));
INSERT INTO `EVENTS` (K, id, field0) VALUES ('0', 1, '0x10');
ASSERT VALUES `EVENTSTEST` (K, ID, FIELD1, FIELD0) VALUES ('0', 1, 'x_0', '0x10');

--@test: project-filter - project and negative filter
CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE double) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM S2 as SELECT K, name, id FROM test where id < -100;
INSERT INTO `TEST` (K, ID, NAME, VALUE) VALUES ('0', 0, 'zero', 0.0);
INSERT INTO `TEST` (K, ID, NAME, VALUE) VALUES ('100', 100, '100', 0.0);
INSERT INTO `TEST` (K, ID, NAME, VALUE) VALUES ('101', -101, '101', 0.0);
ASSERT VALUES `S2` (K, NAME, ID) VALUES ('101', '101', -101);

--@test: project-filter - Json Multi Dimensional Array 2
CREATE STREAM array_array (K STRING KEY, ID BIGINT, ARRAY_COL ARRAY<ARRAY<VARCHAR>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM S3 as SELECT K, ID, ARRAY_COL[1][2] AS array_item FROM array_array;
INSERT INTO `ARRAY_ARRAY` (K, id, array_col) VALUES ('0', 1, ARRAY[ARRAY['item_00_1', 'item_01_1'], ARRAY['item_10_1', 'item_11_1']]);
INSERT INTO `ARRAY_ARRAY` (K, id, array_col) VALUES ('0', 2, ARRAY[ARRAY['item_00_2', 'item_01_2'], ARRAY['item_10_2', 'item_11_2']]);
INSERT INTO `ARRAY_ARRAY` (K, id, array_col) VALUES ('0', 3, ARRAY[ARRAY['item_00_3', 'item_01_3'], ARRAY['item_10_3', 'item_11_3']]);
INSERT INTO `ARRAY_ARRAY` (K, id, array_col) VALUES ('0', 4, ARRAY[ARRAY['item_00_4', 'item_01_4'], ARRAY['item_10_4', 'item_11_4']]);
INSERT INTO `ARRAY_ARRAY` (K, id, array_col) VALUES ('0', 5, ARRAY[ARRAY['item_00_5'], ARRAY['item_10_5', 'item_11_5']]);
ASSERT VALUES `S3` (K, ID, ARRAY_ITEM) VALUES ('0', 1, 'item_01_1');
ASSERT VALUES `S3` (K, ID, ARRAY_ITEM) VALUES ('0', 2, 'item_01_2');
ASSERT VALUES `S3` (K, ID, ARRAY_ITEM) VALUES ('0', 3, 'item_01_3');
ASSERT VALUES `S3` (K, ID, ARRAY_ITEM) VALUES ('0', 4, 'item_01_4');
ASSERT VALUES `S3` (K, ID, ARRAY_ITEM) VALUES ('0', 5, NULL);

--@test: project-filter - Json Multi Dimensional Array
CREATE STREAM array_array (K STRING KEY, ID BIGINT, ARRAY_COL ARRAY<ARRAY<ARRAY<VARCHAR>>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM S3 as SELECT K, ID, ARRAY_COL[1][1][1] AS array_item FROM array_array;
INSERT INTO `ARRAY_ARRAY` (K, id, array_col) VALUES ('0', 1, ARRAY[ARRAY[ARRAY['item_000_1', 'item_001_1'], ARRAY['item_010_1', 'item_011_1']], ARRAY[ARRAY['item_100_1', 'item_101_1'], ARRAY['item_110_1', 'item_111_1']]]);
INSERT INTO `ARRAY_ARRAY` (K, id, array_col) VALUES ('0', 2, ARRAY[ARRAY[ARRAY['item_000_2', 'item_001_2'], ARRAY['item_010_2', 'item_011_2']], ARRAY[ARRAY['item_100_2', 'item_101_2'], ARRAY['item_110_2', 'item_111_2']]]);
INSERT INTO `ARRAY_ARRAY` (K, id, array_col) VALUES ('0', 3, ARRAY[ARRAY[ARRAY['item_000_3', 'item_001_3'], ARRAY['item_010_3', 'item_011_3']], ARRAY[ARRAY['item_100_3', 'item_101_3'], ARRAY['item_110_3', 'item_111_3']]]);
INSERT INTO `ARRAY_ARRAY` (K, id, array_col) VALUES ('0', 4, ARRAY[ARRAY[ARRAY['item_000_4', 'item_001_4'], ARRAY['item_010_4', 'item_011_4']], ARRAY[ARRAY['item_100_4', 'item_101_4'], ARRAY['item_110_4', 'item_111_4']]]);
INSERT INTO `ARRAY_ARRAY` (K, id, array_col) VALUES ('0', 5, ARRAY[ARRAY[ARRAY['item_000_5', 'item_001_5'], ARRAY['item_010_5', 'item_011_5']], ARRAY[ARRAY['item_100_5', 'item_101_5'], ARRAY['item_110_5', 'item_111_5']]]);
ASSERT VALUES `S3` (K, ID, ARRAY_ITEM) VALUES ('0', 1, 'item_000_1');
ASSERT VALUES `S3` (K, ID, ARRAY_ITEM) VALUES ('0', 2, 'item_000_2');
ASSERT VALUES `S3` (K, ID, ARRAY_ITEM) VALUES ('0', 3, 'item_000_3');
ASSERT VALUES `S3` (K, ID, ARRAY_ITEM) VALUES ('0', 4, 'item_000_4');
ASSERT VALUES `S3` (K, ID, ARRAY_ITEM) VALUES ('0', 5, 'item_000_5');

--@test: project-filter - Filter on long literal
CREATE STREAM TEST (K STRING KEY, C1 BIGINT, C2 INTEGER, C3 STRING) WITH (KAFKA_TOPIC='test_topic', value_format='DELIMITED');
CREATE STREAM S1 AS SELECT * FROM TEST WHERE C1 = 4294967296;
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 123, 456, 'foo');
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 4294967296, 456, 'foo');
ASSERT VALUES `S1` (K, C1, C2, C3) VALUES ('0', 4294967296, 456, 'foo');

--@test: project-filter - Filter on string literal
CREATE STREAM TEST (K STRING KEY, C1 BIGINT, C2 INTEGER, C3 STRING) WITH (KAFKA_TOPIC='test_topic', value_format='DELIMITED');
CREATE STREAM S1 AS SELECT * FROM TEST WHERE C3='foo';
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 123, 456, 'foo');
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 2, 1, 'bar');
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 4294967296, 456, 'foo');
ASSERT VALUES `S1` (K, C1, C2, C3) VALUES ('0', 123, 456, 'foo');
ASSERT VALUES `S1` (K, C1, C2, C3) VALUES ('0', 4294967296, 456, 'foo');

--@test: project-filter - Filter on like pattern
CREATE STREAM TEST (K STRING KEY, C1 BIGINT, C2 INTEGER, C3 STRING) WITH (KAFKA_TOPIC='test_topic', value_format='DELIMITED');
CREATE STREAM S1 AS SELECT * FROM TEST WHERE C3 LIKE 'f%';
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 123, 456, 'foo');
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 2, 1, 'bar');
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 4294967296, 456, 'f');
ASSERT VALUES `S1` (K, C1, C2, C3) VALUES ('0', 123, 456, 'foo');
ASSERT VALUES `S1` (K, C1, C2, C3) VALUES ('0', 4294967296, 456, 'f');

--@test: project-filter - Filter on BETWEEN
CREATE STREAM TEST (K STRING KEY, C1 BIGINT, C2 INTEGER, C3 STRING) WITH (KAFKA_TOPIC='test_topic', value_format='DELIMITED');
CREATE STREAM S1 AS SELECT * FROM TEST WHERE C1 BETWEEN 1 AND 3;
INSERT INTO `TEST` (C1, C2, C3) VALUES (1, 456, 'foo');
INSERT INTO `TEST` (C1, C2, C3) VALUES (2, 1, 'bar');
INSERT INTO `TEST` (C1, C2, C3) VALUES (4, 456, 'f');
ASSERT VALUES `S1` (C1, C2, C3) VALUES (1, 456, 'foo');
ASSERT VALUES `S1` (C1, C2, C3) VALUES (2, 1, 'bar');

--@test: project-filter - Filter on NOT BETWEEN
CREATE STREAM TEST (K STRING KEY, C1 BIGINT, C2 INTEGER, C3 STRING) WITH (KAFKA_TOPIC='test_topic', value_format='DELIMITED');
CREATE STREAM S1 AS SELECT * FROM TEST WHERE C1 NOT BETWEEN 1 AND 3;
INSERT INTO `TEST` (C1, C2, C3) VALUES (1, 456, 'foo');
INSERT INTO `TEST` (C1, C2, C3) VALUES (2, 1, 'bar');
INSERT INTO `TEST` (C1, C2, C3) VALUES (4, 456, 'f');
ASSERT VALUES `S1` (C1, C2, C3) VALUES (4, 456, 'f');

--@test: project-filter - Filter on NULL
CREATE STREAM TEST (K STRING KEY, C1 BIGINT, C2 INTEGER, C3 STRING) WITH (KAFKA_TOPIC='test_topic', value_format='DELIMITED');
CREATE STREAM S1 AS SELECT * FROM TEST WHERE C1 IS NULL;
INSERT INTO `TEST` (C1, C2, C3) VALUES (NULL, 456, 'foo');
INSERT INTO `TEST` (C1, C2, C3) VALUES (NULL, 1, 'bar');
INSERT INTO `TEST` (C1, C2, C3) VALUES (4, 456, 'f');
ASSERT VALUES `S1` (C1, C2, C3) VALUES (NULL, 456, 'foo');
ASSERT VALUES `S1` (C1, C2, C3) VALUES (NULL, 1, 'bar');

--@test: project-filter - Filter on NOT NULL
CREATE STREAM TEST (K STRING KEY, C1 BIGINT, C2 INTEGER, C3 STRING) WITH (KAFKA_TOPIC='test_topic', value_format='DELIMITED');
CREATE STREAM S1 AS SELECT * FROM TEST WHERE C1 IS NOT NULL;
INSERT INTO `TEST` (C1, C2, C3) VALUES (NULL, 456, 'foo');
INSERT INTO `TEST` (C1, C2, C3) VALUES (NULL, 1, 'bar');
INSERT INTO `TEST` (C1, C2, C3) VALUES (4, 456, 'f');
ASSERT VALUES `S1` (C1, C2, C3) VALUES (4, 456, 'f');

--@test: project-filter - Filter on IS DISTINCT FROM
CREATE STREAM TEST (K STRING KEY, C1 BIGINT, C2 INTEGER) WITH (KAFKA_TOPIC='test_topic', value_format='DELIMITED');
CREATE STREAM S1 AS SELECT * FROM TEST WHERE C1 IS DISTINCT FROM c2;
INSERT INTO `TEST` (C1, C2) VALUES (0, 0);
INSERT INTO `TEST` (C1, C2) VALUES (0, 1);
INSERT INTO `TEST` (C1, C2) VALUES (1, 0);
INSERT INTO `TEST` (C1, C2) VALUES (0, NULL);
INSERT INTO `TEST` (C1, C2) VALUES (NULL, 0);
INSERT INTO `TEST` (C1, C2) VALUES (NULL, NULL);
ASSERT VALUES `S1` (C1, C2) VALUES (0, 1);
ASSERT VALUES `S1` (C1, C2) VALUES (1, 0);
ASSERT VALUES `S1` (C1, C2) VALUES (0, NULL);
ASSERT VALUES `S1` (C1, C2) VALUES (NULL, 0);

--@test: project-filter - Filter on IS NOT DISTINCT FROM
CREATE STREAM TEST (K STRING KEY, C1 BIGINT, C2 INTEGER) WITH (KAFKA_TOPIC='test_topic', value_format='DELIMITED');
CREATE STREAM S1 AS SELECT * FROM TEST WHERE C1 IS NOT DISTINCT FROM c2;
INSERT INTO `TEST` (C1, C2) VALUES (0, 0);
INSERT INTO `TEST` (C1, C2) VALUES (0, 1);
INSERT INTO `TEST` (C1, C2) VALUES (1, 0);
INSERT INTO `TEST` (C1, C2) VALUES (0, NULL);
INSERT INTO `TEST` (C1, C2) VALUES (NULL, 0);
INSERT INTO `TEST` (C1, C2) VALUES (NULL, NULL);
ASSERT VALUES `S1` (C1, C2) VALUES (0, 0);
ASSERT VALUES `S1` (C1, C2) VALUES (NULL, NULL);

--@test: project-filter - Filter on like pattern without wildcards
CREATE STREAM TEST (K STRING KEY, C1 BIGINT, C2 INTEGER, C3 STRING) WITH (KAFKA_TOPIC='test_topic', value_format='DELIMITED');
CREATE STREAM S1 AS SELECT * FROM TEST WHERE C3 LIKE 'f';
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 123, 456, 'foo');
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 2, 1, 'bar');
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 4294967296, 456, 'f');
ASSERT VALUES `S1` (K, C1, C2, C3) VALUES ('0', 4294967296, 456, 'f');

--@test: project-filter - Null row filter
CREATE STREAM TEST (K STRING KEY, ID bigint, THING MAP<VARCHAR, VARCHAR>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM S3 as SELECT K, ID FROM TEST WHERE THING['status']='false';
INSERT INTO `TEST` (K, id, thing) VALUES ('0', 1, MAP('other':='11', 'status':='false'));
INSERT INTO `TEST` (K) VALUES ('0');
ASSERT VALUES `S3` (K, ID) VALUES ('0', 1);

--@test: project-filter - Filter on not like pattern
CREATE STREAM TEST (K STRING KEY, C1 BIGINT, C2 INTEGER, C3 STRING) WITH (KAFKA_TOPIC='test_topic', value_format='DELIMITED');
CREATE STREAM S1 AS SELECT * FROM TEST WHERE C3 NOT LIKE 'f%';
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 123, 456, 'foo');
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 3, 5, 'bar');
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 4294967296, 456, 'f');
ASSERT VALUES `S1` (K, C1, C2, C3) VALUES ('0', 3, 5, 'bar');

--@test: project-filter - Project fields with reserved name
CREATE STREAM TEST (K STRING KEY, START STRING, `END` STRING) WITH (KAFKA_TOPIC='test_topic', value_format='JSON');
CREATE STREAM S1 AS SELECT K, `END` FROM TEST;
INSERT INTO `TEST` (K, START, `END`) VALUES ('0', 'hello', 'foo');
INSERT INTO `TEST` (K, START, `END`) VALUES ('0', 'world', 'bar');
ASSERT VALUES `S1` (K, `END`) VALUES ('0', 'foo');
ASSERT VALUES `S1` (K, `END`) VALUES ('0', 'bar');

--@test: project-filter - Project struct fields with reserved name
CREATE STREAM TEST (K STRING KEY, S STRUCT<START STRING, `END` STRING>) WITH (KAFKA_TOPIC='test_topic', value_format='JSON');
CREATE STREAM S1 AS SELECT K, S->`END` FROM TEST;
INSERT INTO `TEST` (K, S) VALUES ('0', STRUCT(START:='hello', `END`:='foo'));
INSERT INTO `TEST` (K, S) VALUES ('0', STRUCT(START:='world', `END`:='bar'));
ASSERT VALUES `S1` (K, `END`) VALUES ('0', 'foo');
ASSERT VALUES `S1` (K, `END`) VALUES ('0', 'bar');

--@test: project-filter - CSAS with custom Kafka topic name
CREATE STREAM TEST (K STRING KEY, C1 BIGINT, C2 INTEGER, C3 STRING) WITH (KAFKA_TOPIC='test_topic', value_format='DELIMITED');
CREATE STREAM S1 WITH (KAFKA_TOPIC='topic_s') AS SELECT * FROM TEST WHERE C1 = 4294967296;
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 123, 456, 'foo');
INSERT INTO `TEST` (K, C1, C2, C3) VALUES ('0', 4294967296, 456, 'foo');
ASSERT VALUES `S1` (K, C1, C2, C3) VALUES ('0', 4294967296, 456, 'foo');

--@test: project-filter - Filter on non-STRING key
CREATE STREAM INPUT (K DOUBLE KEY, C1 BIGINT) WITH (KAFKA_TOPIC='test_topic', value_format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE K > 0.1;
INSERT INTO `INPUT` (K, C1) VALUES (1.1, 0);
INSERT INTO `INPUT` (K, C1) VALUES (0.09999, 1);
INSERT INTO `INPUT` (K, C1) VALUES (0.0, 2);
INSERT INTO `INPUT` (K, C1) VALUES (0.11, 3);
INSERT INTO `INPUT` (K, C1) VALUES (1.1, 4);
ASSERT VALUES `OUTPUT` (K, C1) VALUES (1.1, 0);
ASSERT VALUES `OUTPUT` (K, C1) VALUES (0.11, 3);
ASSERT VALUES `OUTPUT` (K, C1) VALUES (1.1, 4);

--@test: project-filter - project nulls
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT ID, name FROM INPUT;
INSERT INTO `INPUT` (ID, NAME) VALUES (1, 'Nick');
INSERT INTO `INPUT` (NAME) VALUES ('null key');
INSERT INTO `INPUT` (ID) VALUES (2);
INSERT INTO `INPUT` (ID) VALUES (3);
INSERT INTO `INPUT` (ID, NAME) VALUES (4, 'Fred');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (1, 'Nick');
ASSERT VALUES `OUTPUT` (NAME) VALUES ('null key');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (2, NULL);
ASSERT VALUES `OUTPUT` (ID) VALUES (3);
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (4, 'Fred');

--@test: project-filter - filter nulls
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM INPUT WHERE ID IS NOT NULL AND NAME IS NOT NULL;
INSERT INTO `INPUT` (ID, NAME) VALUES (1, 'Nick');
INSERT INTO `INPUT` (NAME) VALUES ('null key');
INSERT INTO `INPUT` (ID) VALUES (2);
INSERT INTO `INPUT` (ID) VALUES (3);
INSERT INTO `INPUT` (ID, NAME) VALUES (4, 'Fred');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (1, 'Nick');
ASSERT VALUES `OUTPUT` (ID, NAME) VALUES (4, 'Fred');

--@test: project-filter - projection with aliased key column
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT ID AS NEW_KEY, NAME FROM INPUT;
INSERT INTO `INPUT` (ID, NAME) VALUES (1, 'Nick');
ASSERT VALUES `OUTPUT` (NEW_KEY, NAME) VALUES (1, 'Nick');
ASSERT stream OUTPUT (NEW_KEY INT KEY, NAME STRING) WITH (KAFKA_TOPIC='OUTPUT', VALUE_FORMAT='JSON');

--@test: project-filter - key in projection more than once
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The projection contains a key column (`ID`) more than once, aliased as: ID and ID2.
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT ID, ID AS ID2, NAME FROM INPUT;
--@test: project-filter - projection with missing key column
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the key column ID in its projection (eg, SELECT ID...).
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT NAME FROM INPUT;
--@test: project-filter - projection with missing multi-key column
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the key columns ID and ID2 in its projection (eg, SELECT ID, ID2...).
CREATE STREAM INPUT (ID INT KEY, ID2 INT KEY, NAME STRING) WITH (kafka_topic='test_topic', format='JSON');
CREATE STREAM OUTPUT as SELECT ID, NAME FROM INPUT;
--@test: project-filter - projection with missing key column - with value column with same name
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the key column ID in its projection (eg, SELECT ID...).
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT NAME AS ID, NAME FROM INPUT;
--@test: project-filter - projection no value columns
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The projection contains no value columns.
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT ID FROM INPUT;
--@test: project-filter - project with value column aliased multiple times
CREATE STREAM INPUT (ID INT KEY, NAME STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT as SELECT ID, name, name as name2 FROM INPUT;
INSERT INTO `INPUT` (ID, NAME) VALUES (1, 'Nick');
ASSERT VALUES `OUTPUT` (ID, NAME, NAME2) VALUES (1, 'Nick', 'Nick');

