CREATE STREAM TEST (ID bigint, NAME varchar, VALUE double) WITH (kafka_topic='test_topic', value_format='DELIMITED', key='ID');
CREATE STREAM S1 as SELECT myudf(name) FROM test;