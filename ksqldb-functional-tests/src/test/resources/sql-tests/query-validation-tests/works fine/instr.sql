--@test: instr - test instr with just substring - JSON
CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, INSTR(source, 'or') AS INSTR FROM TEST;
INSERT INTO `TEST` (source) VALUES ('corporate floor');
INSERT INTO `TEST` (source) VALUES ('should I stay or should I go');
INSERT INTO `TEST` (source) VALUES ('short');
INSERT INTO `TEST` (source) VALUES ('no substring');
ASSERT VALUES `OUTPUT` (INSTR) VALUES (2);
ASSERT VALUES `OUTPUT` (INSTR) VALUES (15);
ASSERT VALUES `OUTPUT` (INSTR) VALUES (3);
ASSERT VALUES `OUTPUT` (INSTR) VALUES (0);

--@test: instr - test instr with position - JSON
CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, INSTR(source, 'or', 4) AS INSTR FROM TEST;
INSERT INTO `TEST` (source) VALUES ('corporate floor');
INSERT INTO `TEST` (source) VALUES ('short');
ASSERT VALUES `OUTPUT` (INSTR) VALUES (5);
ASSERT VALUES `OUTPUT` (INSTR) VALUES (0);

--@test: instr - test instr with position and occurrence - JSON
CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, INSTR(source, 'or', 4, 2) AS INSTR FROM TEST;
INSERT INTO `TEST` (source) VALUES ('corporate floor');
INSERT INTO `TEST` (source) VALUES ('short');
ASSERT VALUES `OUTPUT` (INSTR) VALUES (14);
ASSERT VALUES `OUTPUT` (INSTR) VALUES (0);

--@test: instr - test instr with negative position - JSON
CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, INSTR(source, 'or', -4) AS INSTR FROM TEST;
INSERT INTO `TEST` (source) VALUES ('corporate floor');
INSERT INTO `TEST` (source) VALUES ('short');
ASSERT VALUES `OUTPUT` (INSTR) VALUES (5);
ASSERT VALUES `OUTPUT` (INSTR) VALUES (0);

--@test: instr - test instr with negative position and occurrence - JSON
CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, INSTR(source, 'or', -4, 2) AS INSTR FROM TEST;
INSERT INTO `TEST` (source) VALUES ('corporate floor');
INSERT INTO `TEST` (source) VALUES ('short');
ASSERT VALUES `OUTPUT` (INSTR) VALUES (2);
ASSERT VALUES `OUTPUT` (INSTR) VALUES (0);

