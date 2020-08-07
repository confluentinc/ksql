CREATE STREAM orders (NAME STRING, DESC STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM S1 AS SELECT backward_concat(NAME, DESC) AS BACKWARD FROM orders EMIT CHANGES;