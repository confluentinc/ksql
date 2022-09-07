--@test: to_json_string - convert INT to JSON string
CREATE STREAM test (k STRING KEY, v INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM output AS SELECT k, TO_JSON_STRING(v) AS json_v FROM test;
INSERT INTO `TEST` (K, v) VALUES ('k', 1);
INSERT INTO `TEST` (K, v) VALUES ('k', NULL);
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', '1');
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', 'null');

--@test: to_json_string - convert VARCHAR to JSON string
CREATE STREAM test (k STRING KEY, v VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM output AS SELECT k, TO_JSON_STRING(v) AS JSON_v FROM test;
INSERT INTO `TEST` (K, v) VALUES ('k', 'abc');
INSERT INTO `TEST` (K, v) VALUES ('k', NULL);
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', '"abc"');
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', 'null');

--@test: to_json_string - convert BOOLEAN to JSON string
CREATE STREAM test (k STRING KEY, v BOOLEAN) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM output AS SELECT k, TO_JSON_STRING(v) AS JSON_v FROM test;
INSERT INTO `TEST` (K, v) VALUES ('k', true);
INSERT INTO `TEST` (K, v) VALUES ('k', NULL);
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', 'true');
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', 'null');

--@test: to_json_string - convert BIGINT to JSON string
CREATE STREAM test (k STRING KEY, v BIGINT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM output AS SELECT k, TO_JSON_STRING(v) AS JSON_v FROM test;
INSERT INTO `TEST` (K, v) VALUES ('k', 123);
INSERT INTO `TEST` (K, v) VALUES ('k', NULL);
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', '123');
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', 'null');

--@test: to_json_string - convert DOUBLE to JSON string
CREATE STREAM test (k STRING KEY, v DOUBLE) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM output AS SELECT k, TO_JSON_STRING(v) AS JSON_v FROM test;
INSERT INTO `TEST` (K, v) VALUES ('k', 123.456);
INSERT INTO `TEST` (K, v) VALUES ('k', NULL);
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', '123.456');
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', 'null');

--@test: to_json_string - convert DECIMAL to JSON string
CREATE STREAM test (k STRING KEY, v DECIMAL(6, 3)) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM output AS SELECT k, TO_JSON_STRING(v) AS JSON_v FROM test;
INSERT INTO `TEST` (K, v) VALUES ('k', 123.456);
INSERT INTO `TEST` (K, v) VALUES ('k', NULL);
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', '123.456');
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', 'null');

--@test: to_json_string - convert BYTES to JSON string
CREATE STREAM test (k STRING KEY, v BYTES) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM output AS SELECT k, TO_JSON_STRING(v) AS JSON_v FROM test;
INSERT INTO `TEST` (K, v) VALUES ('k', 'IQ==');
INSERT INTO `TEST` (K, v) VALUES ('k', NULL);
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', '"IQ=="');
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', 'null');

--@test: to_json_string - convert TIME to JSON string
CREATE STREAM test (k STRING KEY, v TIME) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM output AS SELECT k, TO_JSON_STRING(v) AS JSON_v FROM test;
INSERT INTO `TEST` (K, v) VALUES ('k', '16:43:40');
INSERT INTO `TEST` (K, v) VALUES ('k', NULL);
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', '"16:43:40"');
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', 'null');

--@test: to_json_string - convert DATE to JSON string
CREATE STREAM test (k STRING KEY, v DATE) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM output AS SELECT k, TO_JSON_STRING(v) AS JSON_v FROM test;
INSERT INTO `TEST` (K, v) VALUES ('k', '1972-09-27');
INSERT INTO `TEST` (K, v) VALUES ('k', NULL);
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', '"1972-09-27"');
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', 'null');

--@test: to_json_string - convert TIMESTAMP to JSON string
CREATE STREAM test (k STRING KEY, v TIMESTAMP) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM output AS SELECT k, TO_JSON_STRING(v) AS JSON_v FROM test;
INSERT INTO `TEST` (K, v) VALUES ('k', '1971-11-28T23:46:40.000');
INSERT INTO `TEST` (K, v) VALUES ('k', NULL);
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', '"1971-11-28T23:46:40.000"');
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', 'null');

--@test: to_json_string - convert ARRAY to JSON string
CREATE STREAM test (k STRING KEY, v ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM output AS SELECT k, TO_JSON_STRING(v) AS JSON_v FROM test;
INSERT INTO `TEST` (K, v) VALUES ('k', ARRAY[1, 2, 3]);
INSERT INTO `TEST` (K, v) VALUES ('k', NULL);
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', '[1,2,3]');
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', 'null');

--@test: to_json_string - convert STRUCT to JSON string
CREATE STREAM test (k STRING KEY, v STRUCT<ID INT, NAME VARCHAR>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM output AS SELECT k, TO_JSON_STRING(v) AS JSON_v FROM test;
INSERT INTO `TEST` (K, v) VALUES ('k', STRUCT(ID:=1, NAME:='Alice'));
INSERT INTO `TEST` (K, v) VALUES ('k', NULL);
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', '{"ID":1,"NAME":"Alice"}');
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', 'null');

--@test: to_json_string - convert MAP to JSON string
CREATE STREAM test (k STRING KEY, v MAP<VARCHAR, INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM output AS SELECT k, TO_JSON_STRING(v) AS JSON_v FROM test;
INSERT INTO `TEST` (K, v) VALUES ('k', MAP('id':=1));
INSERT INTO `TEST` (K, v) VALUES ('k', NULL);
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', '{"id":1}');
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', 'null');

--@test: to_json_string - convert nested structure to JSON string
CREATE STREAM test (k STRING KEY, v MAP<VARCHAR, ARRAY<INT>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM output AS SELECT k, TO_JSON_STRING(v) AS JSON_v FROM test;
INSERT INTO `TEST` (K, v) VALUES ('k', MAP('id':=ARRAY[1, 2, 3]));
INSERT INTO `TEST` (K, v) VALUES ('k', NULL);
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', '{"id":[1,2,3]}');
ASSERT VALUES `OUTPUT` (K, json_v) VALUES ('k', 'null');

