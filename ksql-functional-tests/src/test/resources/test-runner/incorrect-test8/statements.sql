CREATE STREAM TEST (ID int, NAME varchar, VALUE double) WITH (kafka_topic='test_topic', value_format='DELIMITED', key='ID');
INSERT INTO TEST2 (ID, NAME) VALUES (123, 'bar');
CREATE STREAM S1 as SELECT name, value FROM test where id > 100;