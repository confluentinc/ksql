CREATE STREAM TEST (ID int, NAME varchar KEY, VALUE double) WITH (kafka_topic='test_topic', value_format='DELIMITED');
-- This statement will fail, but its not the last statement, which should generate a meta error:
INSERT INTO TEST VALUES ('abc', 101, 'abc', 13.54);
CREATE STREAM OUTPUT AS SELECT * FROM TEST;