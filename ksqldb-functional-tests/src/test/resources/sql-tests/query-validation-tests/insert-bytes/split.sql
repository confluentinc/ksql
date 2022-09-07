--@test: split - split a message by using the '.' delimiter
CREATE STREAM TEST (K STRING KEY, message VARCHAR, b BYTES) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, SPLIT(message, '.') as split_msg, SPLIT(b, TO_BYTES('.', 'utf8')) as split_bytes FROM TEST;
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('1', 'a.b.c', 'YS5iLmM=', 0);
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('2', '.abc.', 'LmFiYy4=', 0);
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('3', '..a..', 'Li5hLi4=', 0);
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('4', 'abc', 'YWJj', 0);
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('5', '', '', 0);
ASSERT VALUES `OUTPUT` (K, SPLIT_MSG, SPLIT_BYTES, ROWTIME) VALUES ('1', ARRAY['a', 'b', 'c'], ARRAY['YQ==', 'Yg==', 'Yw=='], 0);
ASSERT VALUES `OUTPUT` (K, SPLIT_MSG, SPLIT_BYTES, ROWTIME) VALUES ('2', ARRAY['', 'abc', ''], ARRAY['', 'YWJj', ''], 0);
ASSERT VALUES `OUTPUT` (K, SPLIT_MSG, SPLIT_BYTES, ROWTIME) VALUES ('3', ARRAY['', '', 'a', '', ''], ARRAY['', '', 'YQ==', '', ''], 0);
ASSERT VALUES `OUTPUT` (K, SPLIT_MSG, SPLIT_BYTES, ROWTIME) VALUES ('4', ARRAY['abc'], ARRAY['YWJj'], 0);
ASSERT VALUES `OUTPUT` (K, SPLIT_MSG, SPLIT_BYTES, ROWTIME) VALUES ('5', ARRAY[''], ARRAY[''], 0);

--@test: split - split a message by using the '$$' delimiter
CREATE STREAM TEST (K STRING KEY, message VARCHAR, b BYTES) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, SPLIT(message, '$$') as split_msg, SPLIT(b, TO_BYTES('$$', 'utf8')) as split_bytes FROM TEST;
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('1', 'a$$b.c', 'YSQkYi5j', 0);
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('2', '.abc$$', 'LmFiYyQk', 0);
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('3', '.$$a..', 'LiQkYS4u', 0);
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('4', 'abc', 'YWJj', 0);
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('5', '', '', 0);
ASSERT VALUES `OUTPUT` (K, SPLIT_MSG, SPLIT_BYTES, ROWTIME) VALUES ('1', ARRAY['a', 'b.c'], ARRAY['YQ==', 'Yi5j'], 0);
ASSERT VALUES `OUTPUT` (K, SPLIT_MSG, SPLIT_BYTES, ROWTIME) VALUES ('2', ARRAY['.abc', ''], ARRAY['LmFiYw==', ''], 0);
ASSERT VALUES `OUTPUT` (K, SPLIT_MSG, SPLIT_BYTES, ROWTIME) VALUES ('3', ARRAY['.', 'a..'], ARRAY['Lg==', 'YS4u'], 0);
ASSERT VALUES `OUTPUT` (K, SPLIT_MSG, SPLIT_BYTES, ROWTIME) VALUES ('4', ARRAY['abc'], ARRAY['YWJj'], 0);
ASSERT VALUES `OUTPUT` (K, SPLIT_MSG, SPLIT_BYTES, ROWTIME) VALUES ('5', ARRAY[''], ARRAY[''], 0);

--@test: split - split all characters by using the '' delimiter
CREATE STREAM TEST (K STRING KEY, message VARCHAR, b BYTES) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, SPLIT(message, '') as split_msg, SPLIT(b, TO_BYTES('', 'utf8')) as split_bytes FROM TEST;
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('1', 'a.b.c', 'YS5iLmM=', 0);
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('2', '.abc.', 'LmFiYy4=', 0);
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('3', '..a..', 'Li5hLi4=', 0);
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('4', 'abc', 'YWJj', 0);
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('5', '', '', 0);
ASSERT VALUES `OUTPUT` (K, SPLIT_MSG, SPLIT_BYTES, ROWTIME) VALUES ('1', ARRAY['a', '.', 'b', '.', 'c'], ARRAY['YQ==', 'Lg==', 'Yg==', 'Lg==', 'Yw=='], 0);
ASSERT VALUES `OUTPUT` (K, SPLIT_MSG, SPLIT_BYTES, ROWTIME) VALUES ('2', ARRAY['.', 'a', 'b', 'c', '.'], ARRAY['Lg==', 'YQ==', 'Yg==', 'Yw==', 'Lg=='], 0);
ASSERT VALUES `OUTPUT` (K, SPLIT_MSG, SPLIT_BYTES, ROWTIME) VALUES ('3', ARRAY['.', '.', 'a', '.', '.'], ARRAY['Lg==', 'Lg==', 'YQ==', 'Lg==', 'Lg=='], 0);
ASSERT VALUES `OUTPUT` (K, SPLIT_MSG, SPLIT_BYTES, ROWTIME) VALUES ('4', ARRAY['a', 'b', 'c'], ARRAY['YQ==', 'Yg==', 'Yw=='], 0);
ASSERT VALUES `OUTPUT` (K, SPLIT_MSG, SPLIT_BYTES, ROWTIME) VALUES ('5', ARRAY[''], ARRAY[''], 0);

--@test: split - split a message by commas and display pos 0 and 2 of the returned array
CREATE STREAM TEST (K STRING KEY, message VARCHAR, b BYTES) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, SPLIT(message, ',')[1] as s1, SPLIT(message, ',')[3] as s2, SPLIT(b, TO_BYTES(',', 'utf8'))[1] as b1, SPLIT(b, TO_BYTES(',', 'utf8'))[3] as b2 FROM TEST;
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('1', 'a,b,c', 'YSxiLGM=', 0);
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('2', ',A,', 'LEEs', 0);
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('3', 'A,,A', 'QSwsQQ==', 0);
INSERT INTO `TEST` (K, message, b, ROWTIME) VALUES ('4', '1,2,3,4,5', 'MSwyLDMsNCw1', 0);
ASSERT VALUES `OUTPUT` (K, S1, S2, B1, B2, ROWTIME) VALUES ('1', 'a', 'c', 'YQ==', 'Yw==', 0);
ASSERT VALUES `OUTPUT` (K, S1, S2, B1, B2, ROWTIME) VALUES ('2', '', '', '', '', 0);
ASSERT VALUES `OUTPUT` (K, S1, S2, B1, B2, ROWTIME) VALUES ('3', 'A', 'A', 'QQ==', 'QQ==', 0);
ASSERT VALUES `OUTPUT` (K, S1, S2, B1, B2, ROWTIME) VALUES ('4', '1', '3', 'MQ==', 'Mw==', 0);

--@test: split - regexp_split_to_array
CREATE STREAM TEST (K STRING KEY, input_string VARCHAR, pattern VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, REGEXP_SPLIT_TO_ARRAY(input_string, pattern) AS EXTRACTED FROM TEST;
INSERT INTO `TEST` (input_string, pattern) VALUES ('aabcda', '(ab|cd)');
INSERT INTO `TEST` (input_string, pattern) VALUES ('aabdcda', '(ab|cd)');
INSERT INTO `TEST` (input_string, pattern) VALUES ('zxy', '(ab|cd)');
INSERT INTO `TEST` (input_string, pattern) VALUES (NULL, '(ab|cd)');
INSERT INTO `TEST` (input_string, pattern) VALUES ('zxy', NULL);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (ARRAY['a', '', 'a']);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (ARRAY['a', 'd', 'a']);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (ARRAY['zxy']);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (NULL);
ASSERT VALUES `OUTPUT` (EXTRACTED) VALUES (NULL);

