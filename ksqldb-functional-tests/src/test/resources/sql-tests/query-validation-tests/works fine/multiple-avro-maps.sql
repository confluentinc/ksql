--@test: multiple-avro-maps - project multiple avro maps
CREATE STREAM TEST (M1 MAP<STRING, INT>, M2 MAP<STRING, STRING>) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM SINK as SELECT * FROM test;
INSERT INTO `TEST` (M1, M2) VALUES (MAP('K1':=123), MAP('K2':='FOO'));
ASSERT VALUES `SINK` (M1, M2) VALUES (MAP('K1':=123), MAP('K2':='FOO'));

