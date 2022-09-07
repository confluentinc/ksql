--@test: json_keys - extract keys from a json object
CREATE STREAM test (K STRING KEY, colors_obj STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, JSON_KEYS(colors_obj) AS COLORS FROM test WHERE JSON_KEYS(colors_obj) IS NOT NULL;
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', '{"Red": 3, "Green": 5}', 0);
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', '{"Black": 2}', 0);
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', '{"Pink": 1, "Yellow": 3}', 0);
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', '{"White": 7, "Pink": 8}', 0);
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', '{}', 0);
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', NULL, 0);
INSERT INTO `TEST` (K, colors_obj, ROWTIME) VALUES ('1', '', 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', ARRAY['Red', 'Green'], 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', ARRAY['Black'], 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', ARRAY['Pink', 'Yellow'], 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', ARRAY['White', 'Pink'], 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', ARRAY[], 0);

