CREATE STREAM TEST (K STRING KEY, ID bigint, NAME varchar, VALUE double) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM S1 as SELECT K, name FROM test where id > 100;