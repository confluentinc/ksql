--@test: sink-topic-naming - sink-topic-naming: default topic name is stream name, in upper-case
CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OutPut AS SELECT * FROM TEST;
INSERT INTO `TEST` (K, source) VALUES ('1', 's1');
ASSERT VALUES `OUTPUT` (K, SOURCE) VALUES ('1', 's1');

--@test: sink-topic-naming - sink-topic-naming: use supplied topic name, when supplied
SET 'ksql.output.topic.name.prefix' = 'some-prefix-';CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT WITH(KAFKA_TOPIC = 'Fred') AS SELECT * FROM TEST;
INSERT INTO `TEST` (K, source) VALUES ('1', 's1');
ASSERT VALUES `OUTPUT` (K, SOURCE) VALUES ('1', 's1');

--@test: sink-topic-naming: use prefixed default topic name when property set
SET 'ksql.output.topic.name.prefix' = 'some-prefix-';
CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM TEST;
INSERT INTO `TEST` (K, source) VALUES ('1', 's1');
ASSERT VALUES `OUTPUT` (K, SOURCE) VALUES ('1', 's1');
