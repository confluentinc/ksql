--@test: replace - replace - JSON
SET 'ksql.functions.substring.legacy.args' = 'false';CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, REPLACE(source, 'a', 'o') AS REPLACE, REPLACE(source, null, 'o') AS REPLACE_NULL, REPLACE(source, 'a', null) AS REPLACE_W_NULL FROM TEST;
INSERT INTO `TEST` (source) VALUES ('anaconda');
INSERT INTO `TEST` (source) VALUES (NULL);
INSERT INTO `TEST` (source) VALUES ('popcorn');
ASSERT VALUES `OUTPUT` (REPLACE, REPLACE_NULL, REPLACE_W_NULL) VALUES ('onocondo', NULL, NULL);
ASSERT VALUES `OUTPUT` (REPLACE, REPLACE_NULL, REPLACE_W_NULL) VALUES (NULL, NULL, NULL);
ASSERT VALUES `OUTPUT` (REPLACE, REPLACE_NULL, REPLACE_W_NULL) VALUES ('popcorn', NULL, NULL);

--@test: replace - replace - PROTOBUF
SET 'ksql.functions.substring.legacy.args' = 'false';CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, REPLACE(source, 'a', 'o') AS REPLACE, REPLACE(source, null, 'o') AS REPLACE_NULL, REPLACE(source, 'a', null) AS REPLACE_W_NULL FROM TEST;
INSERT INTO `TEST` (source) VALUES ('anaconda');
INSERT INTO `TEST` (source) VALUES (NULL);
INSERT INTO `TEST` (source) VALUES ('popcorn');
ASSERT VALUES `OUTPUT` (REPLACE, REPLACE_NULL, REPLACE_W_NULL) VALUES ('onocondo', NULL, NULL);
ASSERT VALUES `OUTPUT` (REPLACE, REPLACE_NULL, REPLACE_W_NULL) VALUES (NULL, NULL, NULL);
ASSERT VALUES `OUTPUT` (REPLACE, REPLACE_NULL, REPLACE_W_NULL) VALUES ('popcorn', NULL, NULL);

--@test: replace - regexp_replace
CREATE STREAM TEST (K STRING KEY, input_string VARCHAR, pattern VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, REGEXP_REPLACE(input_string, pattern, 'cat') AS EXTRACTED FROM TEST;
INSERT INTO `TEST` (input_string, pattern) VALUES ('baababaa', '(ab)+');
INSERT INTO `TEST` (input_string, pattern) VALUES ('baabbabaa', '(ab)+');
INSERT INTO `TEST` (input_string, pattern) VALUES (NULL, '(ab)+');
INSERT INTO `TEST` (input_string, pattern) VALUES ('baababaa', NULL);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES ('bacataa');
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES ('bacatbcataa');
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (NULL);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (NULL);

--@test: replace - Extract escaped JSON
CREATE STREAM RAW (message VARCHAR) WITH (kafka_topic='test_topic', value_format='KAFKA');
CREATE STREAM JSONIFIED AS SELECT REPLACE(REPLACE(REPLACE(message, '"text": "{', '"text": {'), '\"', '"'), '"}', '}') FROM RAW;
INSERT INTO `RAW` (MESSAGE) VALUES ('{"messageID": "ID:plato-46377-1596636746117-4:4:1:1:1"');
ASSERT VALUES `JSONIFIED` (KSQL_COL_0) VALUES ('{"messageID": "ID:plato-46377-1596636746117-4:4:1:1:1"');

