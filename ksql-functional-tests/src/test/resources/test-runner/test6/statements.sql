CREATE STREAM TEST (ID bigint, NAME varchar, VALUE double) WITH (kafka_topic='test_topic', value_format='JSON', key='NAME');
INSERT INTO TEST VALUES ('abc', 101, 'abc', 13.54);
INSERT INTO TEST VALUES ('foo', 30, 'foo', 4.5);
INSERT INTO TEST (ID, NAME) VALUES (123, 'bar');
INSERT INTO TEST VALUES ('far', 43245, 'far', 43);
INSERT INTO TEST (NAME, ID, ROWTIME) VALUES ('near', 55501, 100000);
CREATE STREAM S1 as SELECT name, value FROM test where id > 100;