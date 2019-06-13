CREATE STREAM TEST (ID bigint, NAME varchar, VALUE double) WITH (kafka_topic='test_topic', value_format='JSON', key='ID');
INSERT INTO TEST VALUES (101, 'abc', 13.54);
INSERT INTO TEST VALUES (30, 'foo', 4.5);
INSERT INTO TEST (ID, NAME) VALUES (123, 'bar');
INSERT INTO TEST VALUES (43245, 'far', 43);
CREATE STREAM S1 as SELECT name, value FROM test where id > 100;