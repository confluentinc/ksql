--@test: identifiers - aliased source
CREATE STREAM INPUT (K STRING KEY, foo INT, bar INT) WITH (kafka_topic='input_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT I.K, I.FOO, I.BAR FROM INPUT I;
INSERT INTO `INPUT` (foo, bar) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (FOO, BAR) VALUES (1, 2);

--@test: identifiers - aliased source with AS
CREATE STREAM INPUT (K STRING KEY, foo INT, bar INT) WITH (kafka_topic='input_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT I.K, I.FOO, I.BAR FROM INPUT AS I;
INSERT INTO `INPUT` (foo, bar) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (FOO, BAR) VALUES (1, 2);

--@test: identifiers - aliased join source
CREATE STREAM INPUT_1 (FOO INT KEY, bar INT) WITH (kafka_topic='t1', value_format='JSON');
CREATE STREAM INPUT_2 (FOO INT KEY, bar INT) WITH (kafka_topic='t2', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT I1.FOO, I1.BAR, I2.BAR FROM INPUT_1 I1 JOIN INPUT_2 I2 WITHIN 1 MINUTE ON I1.FOO = I2.FOO;
INSERT INTO `INPUT_1` (FOO, bar) VALUES (1, 2);
INSERT INTO `INPUT_2` (FOO, bar) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (I1_FOO, I1_BAR, I2_BAR) VALUES (1, 2, 2);

--@test: identifiers - aliased join source with AS
CREATE STREAM INPUT_1 (FOO INT KEY, bar INT) WITH (kafka_topic='t1', value_format='JSON');
CREATE STREAM INPUT_2 (FOO INT KEY, bar INT) WITH (kafka_topic='t2', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT I1.FOO, I1.BAR, I2.BAR FROM INPUT_1 AS I1 JOIN INPUT_2 AS I2 WITHIN 1 MINUTE ON I1.FOO = I2.FOO;
INSERT INTO `INPUT_1` (FOO, bar) VALUES (1, 2);
INSERT INTO `INPUT_2` (FOO, bar) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (I1_FOO, I1_BAR, I2_BAR) VALUES (1, 2, 2);

--@test: identifiers - aliased left unaliased right
CREATE STREAM INPUT_1 (FOO INT KEY, bar INT) WITH (kafka_topic='t1', value_format='JSON');
CREATE STREAM INPUT_2 (FOO INT KEY, bar INT) WITH (kafka_topic='t2', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT I1.FOO, I1.BAR, INPUT_2.BAR FROM INPUT_1 AS I1 JOIN INPUT_2 WITHIN 1 MINUTE ON I1.FOO = INPUT_2.FOO;
INSERT INTO `INPUT_1` (FOO, bar) VALUES (1, 2);
INSERT INTO `INPUT_2` (FOO, bar) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (I1_FOO, I1_BAR, INPUT_2_BAR) VALUES (1, 2, 2);

--@test: identifiers - unaliased left aliased right
CREATE STREAM INPUT_1 (FOO INT KEY, bar INT) WITH (kafka_topic='t1', value_format='JSON');
CREATE STREAM INPUT_2 (FOO INT KEY, bar INT) WITH (kafka_topic='t2', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT INPUT_1.FOO, INPUT_1.BAR, I2.BAR FROM INPUT_1 JOIN INPUT_2 AS I2 WITHIN 1 MINUTE ON INPUT_1.FOO = I2.FOO;
INSERT INTO `INPUT_1` (FOO, bar) VALUES (1, 2);
INSERT INTO `INPUT_2` (FOO, bar) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (INPUT_1_FOO, INPUT_1_BAR, I2_BAR) VALUES (1, 2, 2);

--@test: identifiers - wildcard select with aliased source
CREATE STREAM INPUT (K STRING KEY, foo INT, bar INT) WITH (kafka_topic='input_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT I;
INSERT INTO `INPUT` (foo, bar) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (FOO, BAR) VALUES (1, 2);

--@test: identifiers - prefixed wildcard select with aliased source
CREATE STREAM INPUT (K STRING KEY, foo INT, bar INT) WITH (kafka_topic='input_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT I.* FROM INPUT I;
INSERT INTO `INPUT` (foo, bar) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (FOO, BAR) VALUES (1, 2);

