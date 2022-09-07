--@test: quoted-identifiers - source fields that require quotes
CREATE STREAM TEST (K STRING KEY, `with.dot` VARCHAR, `*bad!chars*` VARCHAR, `SELECT` VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM TEST;
INSERT INTO `TEST` (`with.dot`, `*bad!chars*`, `SELECT`) VALUES ('popcorn', 'cheetos', 'reserved');
ASSERT VALUES `OUTPUT` (`with.dot`, `*bad!chars*`, `SELECT`) VALUES ('popcorn', 'cheetos', 'reserved');

--@test: quoted-identifiers - sink fields that require quotes
CREATE STREAM TEST (K STRING KEY, a VARCHAR, b VARCHAR, c VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, A as `with.dot`, B as `*bad!chars*`, C as `SELECT` FROM TEST;
INSERT INTO `TEST` (A, B, C) VALUES ('popcorn', 'cheetos', 'reserved');
ASSERT VALUES `OUTPUT` (`with.dot`, `*bad!chars*`, `SELECT`) VALUES ('popcorn', 'cheetos', 'reserved');

--@test: quoted-identifiers - udf using fields that require quotes
CREATE STREAM TEST (K STRING KEY, `SELECT` INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, ABS(`SELECT`) FOO FROM TEST;
INSERT INTO `TEST` (`SELECT`) VALUES (-2);
ASSERT VALUES `OUTPUT` (FOO) VALUES (2);

--@test: quoted-identifiers - math using fields that require quotes
CREATE STREAM TEST (K STRING KEY, `SELECT` INT, `with.dot` INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, `SELECT` * `with.dot` AS FOO FROM TEST;
INSERT INTO `TEST` (`with.dot`, `SELECT`) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (FOO) VALUES (2);

--@test: quoted-identifiers - create table with key that is quoted
CREATE TABLE TEST (`some.key` STRING PRIMARY KEY, v VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT * FROM TEST;
INSERT INTO `TEST` (`some.key`, v) VALUES ('a', 'key');
ASSERT VALUES `OUTPUT` (`some.key`, V) VALUES ('a', 'key');

--@test: quoted-identifiers - partition by quoted field
CREATE STREAM TEST (`old.key` VARCHAR KEY, `some.key` VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM TEST PARTITION BY `some.key`;
INSERT INTO `TEST` (`old.key`, `some.key`) VALUES ('old-key', 'key');
ASSERT VALUES `OUTPUT` (`some.key`, `old.key`) VALUES ('key', 'old-key');

--@test: quoted-identifiers - joins using fields that require quotes
CREATE STREAM L (`the key` STRING KEY, `SELECT` VARCHAR, `field!` VARCHAR) WITH (kafka_topic='left_topic', value_format='JSON');
CREATE TABLE R (`with.dot` STRING PRIMARY KEY, `field 0` VARCHAR) WITH (kafka_topic='right_topic', value_format='JSON');
CREATE STREAM JOINED as SELECT * FROM L LEFT JOIN R ON L.`SELECT` = R.`with.dot`;
INSERT INTO `R` (`with.dot`, `field 0`) VALUES ('1', '3');
INSERT INTO `R` (`with.dot`, `field 0`) VALUES ('2', '4');
INSERT INTO `L` (`the key`, `SELECT`, `field!`) VALUES ('diff', '1', 'A');
ASSERT VALUES `JOINED` (`L_SELECT`, `L_the key`, `L_field!`, `R_with.dot`, `R_field 0`) VALUES ('1', 'diff', 'A', '1', '3');

--@test: quoted-identifiers - source names requiring quotes
CREATE STREAM `foo-source` (K STRING KEY, id VARCHAR) WITH (kafka_topic='foo-source', value_format='JSON');
CREATE STREAM `foo-too` AS SELECT K, `foo-source`.id FROM `foo-source`;
INSERT INTO `foo-source` (id) VALUES ('1');
ASSERT VALUES `foo-too` (ID) VALUES ('1');

--@test: quoted-identifiers - literals with quotes galore
CREATE STREAM input (K STRING KEY, id VARCHAR) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM output AS SELECT K, 'foo', '''foo''' FROM input;
INSERT INTO `INPUT` (id) VALUES ('1');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1) VALUES ('foo', '''foo''');

