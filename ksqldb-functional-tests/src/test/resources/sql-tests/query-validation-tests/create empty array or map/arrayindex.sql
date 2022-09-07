--@test: arrayindex - select the first element of an Array
CREATE STREAM test (ID STRING KEY, colors ARRAY<STRING>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, colors[1] as C FROM test;
INSERT INTO `TEST` (colors) VALUES (ARRAY['Red', 'Green']);
INSERT INTO `TEST` (colors) VALUES (ARRAY['Black']);
INSERT INTO `TEST` (colors) VALUES (ARRAY[NULL, 'Yellow', 'Pink']);
INSERT INTO `TEST` (colors) VALUES (ARRAY[]);
ASSERT VALUES `OUTPUT` (C) VALUES ('Red');
ASSERT VALUES `OUTPUT` (C) VALUES ('Black');
ASSERT VALUES `OUTPUT` (C) VALUES (NULL);
ASSERT VALUES `OUTPUT` (C) VALUES (NULL);

--@test: arrayindex - select the last element of an Array (-1)
CREATE STREAM test (ID STRING KEY, colors ARRAY<STRING>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, colors[-1] as C FROM test;
INSERT INTO `TEST` (colors) VALUES (ARRAY['Red', 'Green']);
INSERT INTO `TEST` (colors) VALUES (ARRAY['Black']);
INSERT INTO `TEST` (colors) VALUES (ARRAY['Pink', 'Yellow', 'Pink']);
INSERT INTO `TEST` (colors) VALUES (ARRAY['White', 'Pink']);
INSERT INTO `TEST` (colors) VALUES (ARRAY['Pink', NULL]);
INSERT INTO `TEST` (colors) VALUES (ARRAY[]);
ASSERT VALUES `OUTPUT` (C) VALUES ('Green');
ASSERT VALUES `OUTPUT` (C) VALUES ('Black');
ASSERT VALUES `OUTPUT` (C) VALUES ('Pink');
ASSERT VALUES `OUTPUT` (C) VALUES ('Pink');
ASSERT VALUES `OUTPUT` (C) VALUES (NULL);
ASSERT VALUES `OUTPUT` (C) VALUES (NULL);

