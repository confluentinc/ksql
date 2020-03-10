CREATE STREAM TEST (ID int, NAME varchar, VALUE double) WITH (kafka_topic='test_topic', value_format='DELIMITED', key='NAME');
INSERT INTO TEST VALUES ('abc', 101, 'abc', 13.54);
INSERT INTO TEST VALUES ('foo', 14.5, 'foo', 4.5);
INSERT INTO TEST (ID, NAME) VALUES (123, 'bar');
CREATE STREAM S1 as SELECT name, value FROM test where id > 100;