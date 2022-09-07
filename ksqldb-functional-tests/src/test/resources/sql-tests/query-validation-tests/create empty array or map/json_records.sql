--@test: json_records - extract records from a json object
CREATE STREAM test (K STRING KEY, colors_obj STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, JSON_RECORDS(colors_obj) AS COLORS FROM test WHERE JSON_RECORDS(colors_obj) IS NOT NULL;
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', '{"Red": 3, "Green": 5}', 0);
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', '{"Black": 2}', 0);
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', '{"Pink": 1, "Yellow": 3}', 0);
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', '{"White": 7, "Pink": 8}', 0);
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', '{"White": "Seven"}', 0);
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', '{}', 0);
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', NULL, 0);
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', '', 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', MAP('Red':='3', 'Green':='5'), 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', MAP('Black':='2'), 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', MAP('Pink':='1', 'Yellow':='3'), 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', MAP('White':='7', 'Pink':='8'), 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', MAP('White':='Seven'), 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', MAP(), 0);

--@test: json_records - extract records from a json objects nested
CREATE STREAM test (K STRING KEY, colors_obj STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, JSON_RECORDS(colors_obj)['Red'] AS COLORS FROM test WHERE JSON_RECORDS(colors_obj) IS NOT NULL;
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', '{"Red": 3, "Green": 5}', 0);
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', '{"Black": 2}', 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', '3', 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', NULL, 0);

