--@test: stream-table-join - Should fail on outer-join
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join type: full-outer join not supported for stream-table join. Got TEST_STREAM [FULL] OUTER JOIN TEST_TABLE.
CREATE STREAM test_stream (id1 BIGINT KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE test_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE STREAM output AS SELECT id1, id2, f1, f2 FROM test_stream FULL OUTER JOIN test_table ON id1 = id2;
--@test: stream-table-join - Should fail on incorrect join order for inner-join
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join order: table-stream joins are not supported; only stream-table joins. Got TEST_TABLE [INNER] JOIN TEST_STREAM.
CREATE TABLE test_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE STREAM test_stream (id2 BIGINT KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, id2, f1, f2 FROM test_table JOIN test_stream ON id1 = id2;
--@test: stream-table-join - Should fail on incorrect join order for left-join
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join order: table-stream joins are not supported; only stream-table joins. Got TEST_TABLE LEFT [OUTER] JOIN TEST_STREAM.
CREATE TABLE test_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE STREAM test_stream (id2 BIGINT KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, id2, f1, f2 FROM test_table LEFT JOIN test_stream ON id1 = id2;
--@test: stream-table-join - Should fail on table-side non-key join attribute for inner-join
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: stream-table joins require to join on the table's primary key. Got TEST_STREAM.ID1 = TEST_TABLE.F2.
CREATE STREAM test_stream (id1 BIGINT KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE test_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE STREAM output AS SELECT id1, id2, f1, f2 FROM test_stream JOIN test_table ON id1 = f2;
--@test: stream-table-join - Should fail on table-side non-key join attribute for left-join
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: stream-table joins require to join on the table's primary key. Got TEST_STREAM.ID1 = TEST_TABLE.F2.
CREATE STREAM test_stream (id1 BIGINT KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='AVRO');
CREATE TABLE test_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='AVRO');
CREATE STREAM output AS SELECT id1, id2, f1, f2 FROM test_stream LEFT JOIN test_table ON id1 = f2;
