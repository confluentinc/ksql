--@test: initcap - do initcap - JSON
SET 'ksql.functions.substring.legacy.args' = 'false';CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, INITCAP(source) AS INITCAP FROM TEST;
INSERT INTO `TEST` (source) VALUES ('some_string');
INSERT INTO `TEST` (source) VALUES (NULL);
INSERT INTO `TEST` (source) VALUES ('the   Quick br0wn fOx');
ASSERT VALUES `OUTPUT` (INITCAP) VALUES ('Some_string');
ASSERT VALUES `OUTPUT` (INITCAP) VALUES (NULL);
ASSERT VALUES `OUTPUT` (INITCAP) VALUES ('The   Quick Br0wn Fox');

--@test: initcap - do initcap - AVRO
SET 'ksql.functions.substring.legacy.args' = 'false';CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT AS SELECT K, INITCAP(source) AS INITCAP FROM TEST;
INSERT INTO `TEST` (source) VALUES ('some_string');
INSERT INTO `TEST` (source) VALUES (NULL);
INSERT INTO `TEST` (source) VALUES ('the   Quick br0wn fOx');
ASSERT VALUES `OUTPUT` (INITCAP) VALUES ('Some_string');
ASSERT VALUES `OUTPUT` (INITCAP) VALUES (NULL);
ASSERT VALUES `OUTPUT` (INITCAP) VALUES ('The   Quick Br0wn Fox');

--@test: initcap - do initcap - PROTOBUFs - PROTOBUF
SET 'ksql.functions.substring.legacy.args' = 'false';CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT AS SELECT K, INITCAP(source) AS INITCAP FROM TEST;
INSERT INTO `TEST` (source) VALUES ('some_string');
INSERT INTO `TEST` (source) VALUES (NULL);
INSERT INTO `TEST` (source) VALUES ('the   Quick br0wn fOx');
ASSERT VALUES `OUTPUT` (INITCAP) VALUES ('Some_string');
ASSERT VALUES `OUTPUT` (INITCAP) VALUES ('');
ASSERT VALUES `OUTPUT` (INITCAP) VALUES ('The   Quick Br0wn Fox');

--@test: initcap - do initcap - PROTOBUFs - PROTOBUF_NOSR
SET 'ksql.functions.substring.legacy.args' = 'false';CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT AS SELECT K, INITCAP(source) AS INITCAP FROM TEST;
INSERT INTO `TEST` (source) VALUES ('some_string');
INSERT INTO `TEST` (source) VALUES (NULL);
INSERT INTO `TEST` (source) VALUES ('the   Quick br0wn fOx');
ASSERT VALUES `OUTPUT` (INITCAP) VALUES ('Some_string');
ASSERT VALUES `OUTPUT` (INITCAP) VALUES ('');
ASSERT VALUES `OUTPUT` (INITCAP) VALUES ('The   Quick Br0wn Fox');

