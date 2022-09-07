--@test: slice - sublist for string list
CREATE STREAM TEST (K STRING KEY, l ARRAY<VARCHAR>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, SLICE(l, 2, 3) as sub FROM TEST;
INSERT INTO `TEST` (l) VALUES (ARRAY['a', 'b', 'c', 'd']);
ASSERT VALUES `OUTPUT` (SUB) VALUES (ARRAY['b', 'c']);

--@test: slice - sublist for list of lists
CREATE STREAM TEST (K STRING KEY, l ARRAY<ARRAY<VARCHAR>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, SLICE(l, 2, 3) as sub FROM TEST;
INSERT INTO `TEST` (l) VALUES (ARRAY[ARRAY['a'], ARRAY['b'], ARRAY['c'], ARRAY['d']]);
ASSERT VALUES `OUTPUT` (SUB) VALUES (ARRAY[ARRAY['b'], ARRAY['c']]);

--@test: slice - sublist for list of maps
CREATE STREAM TEST (K STRING KEY, l ARRAY<MAP<VARCHAR, VARCHAR>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, SLICE(l, 2, 3) as sub FROM TEST;
INSERT INTO `TEST` (l) VALUES (ARRAY[MAP('a':='1'), MAP('b':='2'), MAP('c':='3'), MAP('d':='4')]);
ASSERT VALUES `OUTPUT` (SUB) VALUES (ARRAY[MAP('b':='2'), MAP('c':='3')]);

