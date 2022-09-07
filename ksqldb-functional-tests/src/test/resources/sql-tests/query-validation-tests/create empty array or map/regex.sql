--@test: regex - extract without group
CREATE STREAM TEST (K STRING KEY, input_string VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, REGEXP_EXTRACT('test.', input_string) AS EXTRACTED FROM TEST;
INSERT INTO `TEST` (input_string) VALUES ('some_test_string');
INSERT INTO `TEST` (input_string) VALUES ('anothertest');
INSERT INTO `TEST` (input_string) VALUES ('testa');
INSERT INTO `TEST` (input_string) VALUES ('nothing');
INSERT INTO `TEST` (input_string) VALUES (NULL);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES ('test_');
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (NULL);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES ('testa');
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (NULL);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (NULL);

--@test: regex - extract with group
CREATE STREAM TEST (K STRING KEY, input_string VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, REGEXP_EXTRACT('(.*) (.*)', input_string, 2) AS EXTRACTED FROM TEST;
INSERT INTO `TEST` (input_string) VALUES ('steven zhang');
INSERT INTO `TEST` (input_string) VALUES ('andy coates');
INSERT INTO `TEST` (input_string) VALUES ('victoria xia');
INSERT INTO `TEST` (input_string) VALUES ('apurva mehta');
INSERT INTO `TEST` (input_string) VALUES ('agavra');
INSERT INTO `TEST` (input_string) VALUES (NULL);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES ('zhang');
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES ('coates');
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES ('xia');
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES ('mehta');
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (NULL);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (NULL);

--@test: regex - regex_extract_all without group
CREATE STREAM TEST (K STRING KEY, input_string VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, REGEXP_EXTRACT_ALL('test.', input_string) AS EXTRACTED FROM TEST;
INSERT INTO `TEST` (input_string) VALUES ('some_test_string_testabab');
INSERT INTO `TEST` (input_string) VALUES ('anothertest');
INSERT INTO `TEST` (input_string) VALUES ('testa');
INSERT INTO `TEST` (input_string) VALUES ('nothing');
INSERT INTO `TEST` (input_string) VALUES (NULL);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (ARRAY['test_', 'testa']);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (ARRAY[]);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (ARRAY['testa']);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (ARRAY[]);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (NULL);

--@test: regex - regex_extract_all with group
CREATE STREAM TEST (K STRING KEY, input_string VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, REGEXP_EXTRACT_ALL('(\w+) (\w*)', input_string, 2) AS EXTRACTED FROM TEST;
INSERT INTO `TEST` (input_string) VALUES ('steven zhang andy coates');
INSERT INTO `TEST` (input_string) VALUES ('andy coates victoria xia');
INSERT INTO `TEST` (input_string) VALUES ('victoria xia apurva mehta');
INSERT INTO `TEST` (input_string) VALUES ('apurva mehta agavra ');
INSERT INTO `TEST` (input_string) VALUES ('agavra');
INSERT INTO `TEST` (input_string) VALUES (NULL);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (ARRAY['zhang', 'coates']);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (ARRAY['coates', 'xia']);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (ARRAY['xia', 'mehta']);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (ARRAY['mehta', '']);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (ARRAY[]);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (NULL);

--@test: regex - should support nested regex functions with different signatures
CREATE STREAM TEST (K STRING KEY, input_string VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, REGEXP_EXTRACT('.*', REGEXP_EXTRACT('(.*) (.*)', input_string, 2)) AS EXTRACTED FROM TEST;
INSERT INTO `TEST` (input_string) VALUES ('steven zhang');
INSERT INTO `TEST` (input_string) VALUES ('andy coates');
INSERT INTO `TEST` (input_string) VALUES ('victoria xia');
INSERT INTO `TEST` (input_string) VALUES ('apurva mehta');
INSERT INTO `TEST` (input_string) VALUES ('agavra');
INSERT INTO `TEST` (input_string) VALUES (NULL);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES ('zhang');
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES ('coates');
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES ('xia');
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES ('mehta');
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (NULL);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (NULL);

