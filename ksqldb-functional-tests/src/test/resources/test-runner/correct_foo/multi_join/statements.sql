CREATE STREAM test (
       id STRING KEY,
       lookup1 STRING,
       lookup2 STRING)
  WITH (VALUE_FORMAT='AVRO',
       KAFKA_TOPIC = 'test');

CREATE TABLE lookup1 (
       id STRING PRIMARY KEY,
       value STRING)
  WITH (VALUE_FORMAT='AVRO',
       KAFKA_TOPIC = 'lookup1');

CREATE TABLE lookup2 (
       id STRING PRIMARY KEY,
       value STRING)
  WITH (VALUE_FORMAT='AVRO',
       KAFKA_TOPIC = 'lookup2');

CREATE STREAM test_lookup1_output
    AS
SELECT t.lookup1,
       t.id AS id,
       l1.value AS lookup1_value,
       t.lookup2 AS lookup2
  FROM test t
  JOIN lookup1 l1
    ON t.lookup1 = l1.id;

 CREATE STREAM test_lookup2_output
    AS
SELECT l2.id,
       t.id AS id,
       t.lookup1_value AS loo,
       l2.value AS lookup2_value
  FROM test_lookup1_output t
  JOIN lookup2 l2
    ON t.lookup2 = l2.id;