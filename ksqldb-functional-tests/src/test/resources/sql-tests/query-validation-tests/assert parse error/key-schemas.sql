--@test: key-schemas - stream implicit KAFKA STRING KEY
CREATE STREAM INPUT (K STRING KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
CREATE STREAM OUTPUT as SELECT K, ID, AS_VALUE(K) as KEY FROM INPUT;
INSERT INTO `INPUT` (K, id) VALUES ('1', 1);
INSERT INTO `INPUT` (K, id) VALUES ('1', 2);
INSERT INTO `INPUT` (K, id) VALUES ('', 3);
INSERT INTO `INPUT` (id) VALUES (4);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES ('1', 1, '1');
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES ('1', 2, '1');
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES ('', 3, '');
ASSERT VALUES `OUTPUT` (ID, KEY) VALUES (4, NULL);
ASSERT stream OUTPUT (K STRING KEY, ID BIGINT, KEY STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: key-schemas - table implicit KAFKA STRING KEY
CREATE TABLE INPUT (K STRING PRIMARY KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
CREATE TABLE OUTPUT as SELECT K, ID, AS_VALUE(K) as KEY FROM INPUT;
INSERT INTO `INPUT` (K, id) VALUES ('1', 1);
INSERT INTO `INPUT` (K, id) VALUES ('1', 2);
INSERT INTO `INPUT` (K, id) VALUES ('', 3);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES ('1', 1, '1');
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES ('1', 2, '1');
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES ('', 3, '');
ASSERT table OUTPUT (K STRING PRIMARY KEY, ID BIGINT, KEY STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: key-schemas - stream explicit KAFKA STRING KEY
CREATE STREAM INPUT (K STRING KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
CREATE STREAM OUTPUT as SELECT K, ID, AS_VALUE(K) as KEY FROM INPUT;
INSERT INTO `INPUT` (K, id) VALUES ('1', 1);
INSERT INTO `INPUT` (K, id) VALUES ('1', 2);
INSERT INTO `INPUT` (K, id) VALUES ('', 3);
INSERT INTO `INPUT` (id) VALUES (4);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES ('1', 1, '1');
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES ('1', 2, '1');
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES ('', 3, '');
ASSERT VALUES `OUTPUT` (ID, KEY) VALUES (4, NULL);
ASSERT stream OUTPUT (K STRING KEY, ID BIGINT, KEY STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: key-schemas - table explicit KAFKA STRING KEY
CREATE TABLE INPUT (K STRING PRIMARY KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
CREATE TABLE OUTPUT as SELECT K, ID, AS_VALUE(K) as KEY FROM INPUT;
INSERT INTO `INPUT` (K, id) VALUES ('1', 1);
INSERT INTO `INPUT` (K, id) VALUES ('1', 2);
INSERT INTO `INPUT` (K, id) VALUES ('', 3);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES ('1', 1, '1');
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES ('1', 2, '1');
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES ('', 3, '');
ASSERT table OUTPUT (K STRING PRIMARY KEY, ID BIGINT, KEY STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: key-schemas - stream explicit KAFKA INT KEY
CREATE STREAM INPUT (K INT KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
CREATE STREAM OUTPUT as SELECT K, ID, AS_VALUE(K) as KEY FROM INPUT;
INSERT INTO `INPUT` (K, id) VALUES (3, 1);
INSERT INTO `INPUT` (K, id) VALUES (2, 2);
INSERT INTO `INPUT` (id) VALUES (3);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES (3, 1, 3);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES (2, 2, 2);
ASSERT VALUES `OUTPUT` (ID, KEY) VALUES (3, NULL);
ASSERT stream OUTPUT (K INT KEY, ID BIGINT, KEY INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: key-schemas - table explicit KAFKA INT KEY
CREATE TABLE INPUT (K INT PRIMARY KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
CREATE TABLE OUTPUT as SELECT K, ID, AS_VALUE(K) as KEY FROM INPUT;
INSERT INTO `INPUT` (K, id) VALUES (3, 1);
INSERT INTO `INPUT` (K, id) VALUES (2, 2);
INSERT INTO `INPUT` (K, id) VALUES (1, 3);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES (3, 1, 3);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES (2, 2, 2);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES (1, 3, 1);
ASSERT table OUTPUT (K INT PRIMARY KEY, ID BIGINT, KEY INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: key-schemas - stream explicit KAFKA BIGINT KEY
CREATE STREAM INPUT (K BIGINT KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
CREATE STREAM OUTPUT as SELECT K, ID, AS_VALUE(K) AS KEY FROM INPUT;
INSERT INTO `INPUT` (K, id) VALUES (3, 1);
INSERT INTO `INPUT` (K, id) VALUES (2, 2);
INSERT INTO `INPUT` (id) VALUES (3);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES (3, 1, 3);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES (2, 2, 2);
ASSERT VALUES `OUTPUT` (ID, KEY) VALUES (3, NULL);
ASSERT stream OUTPUT (K BIGINT KEY, ID BIGINT, KEY BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: key-schemas - table explicit KAFKA BIGINT KEY
CREATE TABLE INPUT (K BIGINT PRIMARY KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
CREATE TABLE OUTPUT as SELECT K, ID, AS_VALUE(K) as KEY FROM INPUT;
INSERT INTO `INPUT` (K, id) VALUES (3, 1);
INSERT INTO `INPUT` (K, id) VALUES (2, 2);
INSERT INTO `INPUT` (K, id) VALUES (1, 3);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES (3, 1, 3);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES (2, 2, 2);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES (1, 3, 1);
ASSERT table OUTPUT (K BIGINT PRIMARY KEY, ID BIGINT, KEY BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: key-schemas - stream explicit KAFKA DOUBLE KEY
CREATE STREAM INPUT (K DOUBLE KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
CREATE STREAM OUTPUT as SELECT K, ID, AS_VALUE(K) as KEY FROM INPUT;
INSERT INTO `INPUT` (K, id) VALUES (3.0, 1);
INSERT INTO `INPUT` (K, id) VALUES (2.0, 2);
INSERT INTO `INPUT` (id) VALUES (3);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES (3.0, 1, 3.0);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES (2.0, 2, 2.0);
ASSERT VALUES `OUTPUT` (ID, KEY) VALUES (3, NULL);
ASSERT stream OUTPUT (K DOUBLE KEY, ID BIGINT, KEY DOUBLE) WITH (KAFKA_TOPIC='OUTPUT');

--@test: key-schemas - table explicit KAFKA DOUBLE KEY
CREATE TABLE INPUT (K DOUBLE PRIMARY KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
CREATE TABLE OUTPUT as SELECT K, ID, AS_VALUE(K) as KEY FROM INPUT;
INSERT INTO `INPUT` (K, id) VALUES (3.0, 1);
INSERT INTO `INPUT` (K, id) VALUES (2.0, 2);
INSERT INTO `INPUT` (K, id) VALUES (1.0, 3);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES (3.0, 1, 3.0);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES (2.0, 2, 2.0);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES (1.0, 3, 1.0);
ASSERT table OUTPUT (K DOUBLE PRIMARY KEY, ID BIGINT, KEY DOUBLE) WITH (KAFKA_TOPIC='OUTPUT');

--@test: key-schemas - create stream explicit unsupported KEY type
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Key format does not support schema.
CREATE STREAM INPUT (K BOOLEAN KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
--@test: key-schemas - create table explicit unsupported KEY type
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Key format does not support schema.
CREATE TABLE INPUT (K DECIMAL(21,19) PRIMARY KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
--@test: key-schemas - create stream as explicit unsupported KEY type
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Key format does not support schema.
CREATE STREAM INPUT (K STRING KEY, ID ARRAY<INT>) WITH (kafka_topic='input',value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT PARTITION BY ID;
--@test: key-schemas - create table as explicit unsupported KEY type
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Map keys, including types that contain maps, are not supported as they may lead to unexpected behavior due to inconsistent serialization. Key column name: `ID`. Column type: MAP<STRING, INTEGER>. See https://github.com/confluentinc/ksql/issues/6621 for more.
CREATE STREAM INPUT (K STRING KEY, ID MAP<STRING, INT>) WITH (kafka_topic='input',value_format='JSON');
CREATE TABLE OUTPUT AS SELECT ID, COUNT() FROM INPUT GROUP BY ID;
--@test: key-schemas - explicit key field named other than KEY
CREATE STREAM INPUT (OTHER DOUBLE KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
CREATE STREAM OUTPUT as SELECT OTHER, ID, AS_VALUE(OTHER) as KEY FROM INPUT;
INSERT INTO `INPUT` (OTHER, id) VALUES (3.0, 1);
ASSERT VALUES `OUTPUT` (OTHER, ID, KEY) VALUES (3.0, 1, 3.0);
ASSERT stream OUTPUT (OTHER DOUBLE KEY, ID BIGINT, KEY DOUBLE) WITH (KAFKA_TOPIC='OUTPUT');

--@test: key-schemas - KEY key field name
CREATE STREAM INPUT (KEY STRING KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
CREATE STREAM OUTPUT as SELECT KEY, ID, AS_VALUE(KEY) as KEY2 FROM INPUT;
INSERT INTO `INPUT` (KEY, id) VALUES ('a', 1);
ASSERT VALUES `OUTPUT` (KEY, ID, KEY2) VALUES ('a', 1, 'a');
ASSERT stream OUTPUT (KEY STRING KEY, ID BIGINT, KEY2 STRING) WITH (KAFKA_TOPIC='OUTPUT');

--@test: key-schemas - KEY value field name
CREATE STREAM INPUT (K STRING KEY, KEY STRING, ID bigint) WITH (kafka_topic='input',value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, key, id) VALUES ('1', 'a', 1);
ASSERT VALUES `OUTPUT` (K, ID, KEY) VALUES ('1', 1, 'a');

--@test: key-schemas - windowed table explicit non-STRING KEY
CREATE STREAM INPUT (K BIGINT KEY, VALUE BIGINT) WITH (kafka_topic='input', value_format='JSON');
CREATE TABLE OUTPUT as SELECT K AS ID, max(value) AS MAX FROM INPUT WINDOW TUMBLING (SIZE 30 SECONDS) group by K;
INSERT INTO `INPUT` (K, value) VALUES (10, 1);
INSERT INTO `INPUT` (K, value) VALUES (10, 2);
ASSERT VALUES `OUTPUT` (ID, MAX, WINDOWSTART, WINDOWEND) VALUES (10, 1, 0, 30000);
ASSERT VALUES `OUTPUT` (ID, MAX, WINDOWSTART, WINDOWEND) VALUES (10, 2, 0, 30000);
ASSERT table OUTPUT (`ID` BIGINT PRIMARY KEY, `MAX` BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: key-schemas - windowed table explicit non-STRING KEY udf
CREATE STREAM INPUT (K BIGINT KEY, VALUE BIGINT) WITH (kafka_topic='input', value_format='JSON');
CREATE TABLE OUTPUT as SELECT K, EXP(K) AS EXP, COUNT(1) FROM INPUT WINDOW TUMBLING (SIZE 30 SECONDS) GROUP BY K;
INSERT INTO `INPUT` (K, value) VALUES (10, 1);
ASSERT VALUES `OUTPUT` (K, EXP, KSQL_COL_0, WINDOWSTART, WINDOWEND) VALUES (10, 22026.465794806718, 1, 0, 30000);
ASSERT table OUTPUT (`K` BIGINT PRIMARY KEY, `EXP` DOUBLE, `KSQL_COL_0` BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: key-schemas - filter by non-STRING KEY
CREATE STREAM INPUT (K BIGINT KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM INPUT WHERE K > 10;
INSERT INTO `INPUT` (K, id) VALUES (9, 0);
INSERT INTO `INPUT` (K, id) VALUES (10, 1);
INSERT INTO `INPUT` (K, id) VALUES (11, 2);
INSERT INTO `INPUT` (K, id) VALUES (12, 3);
ASSERT VALUES `OUTPUT` (K, ID) VALUES (11, 2);
ASSERT VALUES `OUTPUT` (K, ID) VALUES (12, 3);

--@test: key-schemas - filter by non-STRING KEY in UDF
CREATE STREAM INPUT (K DOUBLE KEY, ID bigint) WITH (kafka_topic='input',value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM INPUT WHERE EXP(K) >= 1;
INSERT INTO `INPUT` (K, id) VALUES (-0.2, 0);
INSERT INTO `INPUT` (K, id) VALUES (-0.1, 1);
INSERT INTO `INPUT` (K, id) VALUES (0.0, 2);
INSERT INTO `INPUT` (K, id) VALUES (0.1, 3);
ASSERT VALUES `OUTPUT` (K, ID) VALUES (0.0, 2);
ASSERT VALUES `OUTPUT` (K, ID) VALUES (0.1, 3);

--@test: key-schemas - fail on STRING with parameter
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: line 1:32: mismatched input 'KEY' expecting {'STRING', INTEGER_VALUE}
CREATE STREAM INPUT (K STRING (KEY)) WITH (kafka_topic='input',value_format='JSON');
--@test: key-schemas - fail on INTEGER with parameter
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: line 1:33: mismatched input 'KEY' expecting {'STRING', INTEGER_VALUE}
CREATE STREAM INPUT (K INTEGER (KEY)) WITH (kafka_topic='input',value_format='JSON');
--@test: key-schemas - fail on DOUBLE with parameter
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: line 1:32: mismatched input 'KEY' expecting {'STRING', INTEGER_VALUE}
CREATE STREAM INPUT (K DOUBLE (KEY)) WITH (kafka_topic='input',value_format='JSON');
--@test: key-schemas - fail on VARCHAR with parameter
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: line 1:33: mismatched input 'KEY' expecting {'STRING', INTEGER_VALUE}
CREATE STREAM INPUT (K VARCHAR (KEY)) WITH (kafka_topic='input',value_format='JSON');
--@test: key-schemas - fail on INT with parameter
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: line 1:29: mismatched input 'KEY' expecting {'STRING', INTEGER_VALUE}
CREATE STREAM INPUT (K INT (KEY)) WITH (kafka_topic='input',value_format='JSON');
--@test: key-schemas - fail on BOOLEAN with parameter
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: line 1:33: mismatched input 'KEY' expecting {'STRING', INTEGER_VALUE}
CREATE STREAM INPUT (K BOOLEAN (KEY)) WITH (kafka_topic='input',value_format='JSON');
--@test: key-schemas - fail on BIGINT with parameter
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: line 1:32: mismatched input 'KEY' expecting {'STRING', INTEGER_VALUE}
CREATE STREAM INPUT (K BIGINT (KEY)) WITH (kafka_topic='input',value_format='JSON');
--@test: key-schemas - fail on VARCHAR(STRING) with parameter
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: line 1:40: mismatched input '(' expecting {',', ')'}
CREATE STREAM INPUT (K VARCHAR(STRING) (KEY)) WITH (kafka_topic='input',value_format='JSON');
--@test: key-schemas - VARCHAR(STRING) as key and value
CREATE STREAM INPUT (k VARCHAR(STRING) KEY, v1 VARCHAR(STRING)) WITH (kafka_topic='input',value_format='JSON');
CREATE STREAM OUTPUT as SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, v1) VALUES ('bob', 'foo');
ASSERT VALUES `OUTPUT` (K, V1) VALUES ('bob', 'foo');

