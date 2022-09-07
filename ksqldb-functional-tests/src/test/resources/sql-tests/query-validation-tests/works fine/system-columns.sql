--@test: system-columns - should fail if ROWTIME used as column name
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: `ROWTIME` is a reserved column name. You cannot use it as an alias for a column.
CREATE STREAM INPUT (K STRING KEY, x int) WITH (kafka_topic='test', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, x AS rowtime FROM INPUT;
--@test: system-columns - should fail if WINDOWSTART used as column name
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: `WINDOWSTART` is a reserved column name. You cannot use it as an alias for a column.
CREATE STREAM INPUT (K STRING KEY, x int) WITH (kafka_topic='test', value_format='JSON', WINDOW_TYPE='session');
CREATE STREAM OUTPUT AS SELECT K, x AS windowstart FROM INPUT;
--@test: system-columns - should fail if WINDOWEND used as column name
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: `WINDOWEND` is a reserved column name. You cannot use it as an alias for a column.
CREATE STREAM INPUT (K STRING KEY, x int) WITH (kafka_topic='test', value_format='JSON', WINDOW_TYPE='session');
CREATE STREAM OUTPUT AS SELECT K, x AS windowend FROM INPUT;
