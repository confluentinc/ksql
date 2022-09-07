--@test: arraycontains - filter rows where the ARRAY column contains a specified STRING
CREATE STREAM test (ID STRING KEY, colors ARRAY<STRING>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, colors FROM test WHERE ARRAY_CONTAINS(colors, 'Pink');
INSERT INTO `TEST` (colors) VALUES (ARRAY['Red', 'Green']);
INSERT INTO `TEST` (colors) VALUES (ARRAY['Black']);
INSERT INTO `TEST` (colors) VALUES (ARRAY['Pink', 'Yellow', 'Pink']);
INSERT INTO `TEST` (colors) VALUES (ARRAY['White', 'Pink']);
INSERT INTO `TEST` (colors) VALUES (ARRAY['Pink', NULL]);
INSERT INTO `TEST` (colors) VALUES (NULL);
ASSERT VALUES `OUTPUT` (COLORS) VALUES (ARRAY['Pink', 'Yellow', 'Pink']);
ASSERT VALUES `OUTPUT` (COLORS) VALUES (ARRAY['White', 'Pink']);
ASSERT VALUES `OUTPUT` (COLORS) VALUES (ARRAY['Pink', NULL]);

--@test: arraycontains - filter rows where the STRUCT->ARRAY column contains a specified STRING
CREATE STREAM test (ID STRING KEY, c1 STRUCT<colors ARRAY<STRING>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, c1->colors AS colors FROM test WHERE ARRAY_CONTAINS(c1->colors, 'Pink');
INSERT INTO `TEST` (c1) VALUES (STRUCT(colors:=ARRAY['Red', 'Green']));
INSERT INTO `TEST` (c1) VALUES (STRUCT(colors:=ARRAY['Black']));
INSERT INTO `TEST` (c1) VALUES (STRUCT(colors:=ARRAY['Pink', 'Yellow', 'Pink']));
INSERT INTO `TEST` (c1) VALUES (STRUCT(colors:=ARRAY['White', 'Pink']));
INSERT INTO `TEST` (c1) VALUES (STRUCT(colors:=ARRAY['Pink', NULL]));
INSERT INTO `TEST` (c1) VALUES (STRUCT(colors:=NULL));
ASSERT VALUES `OUTPUT` (COLORS) VALUES (ARRAY['Pink', 'Yellow', 'Pink']);
ASSERT VALUES `OUTPUT` (COLORS) VALUES (ARRAY['White', 'Pink']);
ASSERT VALUES `OUTPUT` (COLORS) VALUES (ARRAY['Pink', NULL]);

