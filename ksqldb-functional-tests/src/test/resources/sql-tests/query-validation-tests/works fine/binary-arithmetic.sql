--@test: binary-arithmetic - in projection
CREATE STREAM INPUT (col0 INT KEY, col1 BIGINT, col2 DOUBLE) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT col0, col0+col1, col2+10, col1*25, 12*4+2 FROM INPUT;
INSERT INTO `INPUT` (COL0, col1, col2) VALUES (6, 25, 3.21);
ASSERT VALUES `OUTPUT` (COL0, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3) VALUES (6, 31, 13.21, 625, 50);

