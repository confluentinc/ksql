--@test: windowed-source - Should fail querying windowed table
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: KSQL does not support persistent queries on windowed tables.
CREATE TABLE INPUT (K STRING PRIMARY KEY, x int) WITH (kafka_topic='test', value_format='JSON', WINDOW_TYPE='Session');
CREATE TABLE OUTPUT AS SELECT * FROM INPUT;
--@test: windowed-source - window without group by
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: WINDOW clause requires a GROUP BY clause.
CREATE STREAM INPUT (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM OUTPUT as SELECT * FROM INPUT WINDOW TUMBLING (SIZE 30 SECONDS);
