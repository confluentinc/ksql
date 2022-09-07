--@test: like - literal
CREATE STREAM INPUT (K STRING KEY, id INT, val VARCHAR) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, id FROM INPUT WHERE val LIKE '%foo%';
INSERT INTO `INPUT` (id, val) VALUES (1, 'barfoobaz');
ASSERT VALUES `OUTPUT` (ID) VALUES (1);

--@test: like - exp
CREATE STREAM INPUT (K STRING KEY, id INT, val VARCHAR, pattern VARCHAR) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, id FROM INPUT WHERE val LIKE pattern;
INSERT INTO `INPUT` (id, val, pattern) VALUES (1, 'foo', 'foo');
INSERT INTO `INPUT` (id, val, pattern) VALUES (-1, 'bar', 'foo');
INSERT INTO `INPUT` (id, val, pattern) VALUES (2, 'foo', 'f_o');
INSERT INTO `INPUT` (id, val, pattern) VALUES (3, 'foobar', '%oba%');
ASSERT VALUES `OUTPUT` (ID) VALUES (1);
ASSERT VALUES `OUTPUT` (ID) VALUES (2);
ASSERT VALUES `OUTPUT` (ID) VALUES (3);

--@test: like - escape
CREATE STREAM INPUT (K STRING KEY, id INT, val VARCHAR, pattern VARCHAR) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, id FROM INPUT WHERE val LIKE pattern ESCAPE '!';
INSERT INTO `INPUT` (id, val, pattern) VALUES (1, '%', '!%');
INSERT INTO `INPUT` (id, val, pattern) VALUES (-1, 'x', '!%');
ASSERT VALUES `OUTPUT` (ID) VALUES (1);

