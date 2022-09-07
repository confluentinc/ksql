--@test: count-distinct - count distinct
CREATE STREAM TEST (K STRING KEY, ID varchar, NAME varchar) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, count_distinct(name) as count FROM test group by id;
INSERT INTO `TEST` (id, name) VALUES ('foo', 'one');
INSERT INTO `TEST` (id, name) VALUES ('foo', 'two');
INSERT INTO `TEST` (id, name) VALUES ('foo', 'one');
INSERT INTO `TEST` (id, name) VALUES ('foo', 'two');
INSERT INTO `TEST` (id, name) VALUES ('bar', 'one');
INSERT INTO `TEST` (id, name) VALUES ('foo', NULL);
ASSERT VALUES `S2` (ID, COUNT) VALUES ('foo', 1);
ASSERT VALUES `S2` (ID, COUNT) VALUES ('foo', 2);
ASSERT VALUES `S2` (ID, COUNT) VALUES ('foo', 2);
ASSERT VALUES `S2` (ID, COUNT) VALUES ('foo', 2);
ASSERT VALUES `S2` (ID, COUNT) VALUES ('bar', 1);
ASSERT VALUES `S2` (ID, COUNT) VALUES ('foo', 2);

