CREATE STREAM TEST (ROWKEY BIGINT KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED', key='ID');
CREATE TABLE S2 as SELECT id, max(value) FROM test WINDOW TUMBLING (SIZE 30 SECONDS) group by id;
