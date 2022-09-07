--@test: udaf - throw on no literal expressions pass for literal params
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Parameter 2 passed to function EARLIEST_BY_OFFSET must be a literal constant, but was expression: 'F0'
CREATE STREAM INPUT (ID BIGINT KEY, F0 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT ID, EARLIEST_BY_OFFSET(F0, F0) FROM INPUT GROUP BY ID;
--@test: udaf - support more than one literal param
CREATE STREAM INPUT (ID BIGINT KEY, F0 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT ID, EARLIEST_BY_OFFSET(F0, 2, true) FROM INPUT GROUP BY ID;
INSERT INTO `INPUT` (ID, F0, ROWTIME) VALUES (0, 1, 0);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0, ROWTIME) VALUES (0, ARRAY[1], 0);

