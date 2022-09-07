--@test: json_array_contains - filter rows where the ARRAY column contains a specified STRING
CREATE STREAM test (K STRING KEY, colors STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, colors FROM test WHERE JSON_ARRAY_CONTAINS(colors, 'Pink');
INSERT INTO `TEST` (K, colors, ROWTIME) VALUES ('1', '["Red", "Green"]', 0);
INSERT INTO `TEST` (K, colors, ROWTIME) VALUES ('1', '["Black"]', 0);
INSERT INTO `TEST` (K, colors, ROWTIME) VALUES ('1', '["Pink", "Yellow", "Pink"]', 0);
INSERT INTO `TEST` (K, colors, ROWTIME) VALUES ('1', '["White", "Pink"]', 0);
INSERT INTO `TEST` (K, colors, ROWTIME) VALUES ('1', '["Pink", null]', 0);
INSERT INTO `TEST` (K, colors, ROWTIME) VALUES ('1', NULL, 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', '["Pink", "Yellow", "Pink"]', 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', '["White", "Pink"]', 0);
ASSERT VALUES `OUTPUT` (K, COLORS, ROWTIME) VALUES ('1', '["Pink", null]', 0);

