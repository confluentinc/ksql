--@test: elt-field - elect the second parameter
CREATE STREAM TEST (K STRING KEY, message VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, ELT(2, 'ignored', message) as elem FROM TEST;
INSERT INTO `TEST` (K, message, ROWTIME) VALUES ('1', 'something', 0);
INSERT INTO `TEST` (K, message, ROWTIME) VALUES ('2', NULL, 0);
ASSERT VALUES `OUTPUT` (K, ELEM, ROWTIME) VALUES ('1', 'something', 0);
ASSERT VALUES `OUTPUT` (K, ELEM, ROWTIME) VALUES ('2', NULL, 0);

--@test: elt-field - field the correct parameter
CREATE STREAM TEST (K STRING KEY, a VARCHAR, b VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, FIELD('hello', a, b) as pos FROM TEST;
INSERT INTO `TEST` (K, a, b, ROWTIME) VALUES ('1', 'hello', 'world', 0);
INSERT INTO `TEST` (K, a, b, ROWTIME) VALUES ('2', NULL, 'world', 0);
INSERT INTO `TEST` (K, a, b, ROWTIME) VALUES ('3', 'world', 'hello', 0);
INSERT INTO `TEST` (K, a, b, ROWTIME) VALUES ('4', NULL, NULL, 0);
ASSERT VALUES `OUTPUT` (K, POS, ROWTIME) VALUES ('1', 1, 0);
ASSERT VALUES `OUTPUT` (K, POS, ROWTIME) VALUES ('2', 0, 0);
ASSERT VALUES `OUTPUT` (K, POS, ROWTIME) VALUES ('3', 2, 0);
ASSERT VALUES `OUTPUT` (K, POS, ROWTIME) VALUES ('4', 0, 0);

--@test: elt-field - ELT should undo FIELD
CREATE STREAM TEST (K STRING KEY, a VARCHAR, b VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, ELT(FIELD('hello', a, b), a, b) as hello FROM TEST;
INSERT INTO `TEST` (K, a, b, ROWTIME) VALUES ('1', 'hello', 'world', 0);
INSERT INTO `TEST` (K, a, b, ROWTIME) VALUES ('2', NULL, 'world', 0);
INSERT INTO `TEST` (K, a, b, ROWTIME) VALUES ('3', 'world', 'hello', 0);
INSERT INTO `TEST` (K, a, b, ROWTIME) VALUES ('4', NULL, NULL, 0);
ASSERT VALUES `OUTPUT` (K, HELLO, ROWTIME) VALUES ('1', 'hello', 0);
ASSERT VALUES `OUTPUT` (K, HELLO, ROWTIME) VALUES ('2', NULL, 0);
ASSERT VALUES `OUTPUT` (K, HELLO, ROWTIME) VALUES ('3', 'hello', 0);
ASSERT VALUES `OUTPUT` (K, HELLO, ROWTIME) VALUES ('4', NULL, 0);

