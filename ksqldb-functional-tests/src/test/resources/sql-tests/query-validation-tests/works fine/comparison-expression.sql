--@test: comparison-expression - invalid where predicate - type mismatch
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Error in WHERE expression: Cannot compare ID (INTEGER) to 'not an int' (STRING) with GREATER_THAN_OR_EQUAL.
CREATE STREAM INPUT (K DOUBLE KEY, ID INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID >= 'not an int' EMIT CHANGES;
--@test: comparison-expression - invalid where predicate - type mismatch complex expression
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Error in WHERE expression: Cannot compare ID (INTEGER) to 'not an int' (STRING) with GREATER_THAN_OR_EQUAL.
CREATE STREAM INPUT (K STRING KEY, ID INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE K = 'abc' AND ID >= 'not an int' EMIT CHANGES;
--@test: comparison-expression - invalid where predicate - type mismatch join
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Error in WHERE expression: Cannot compare I1_BAR (INTEGER) to I2_BAR (STRING) with GREATER_THAN.
CREATE STREAM INPUT_1 (K INT KEY, foo INT, bar INT) WITH (kafka_topic='t1', value_format='JSON');
CREATE STREAM INPUT_2 (K INT KEY, foo INT, bar STRING) WITH (kafka_topic='t2', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT I1.FOO, I1.BAR, I2.BAR FROM INPUT_1 I1 JOIN INPUT_2 I2 WITHIN 1 MINUTE ON I1.FOO = I2.FOO WHERE I1.BAR > I2.BAR;
--@test: comparison-expression - invalid where predicate - string literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Type error in WHERE expression: Should evaluate to boolean but is 'not a boolean' (STRING) instead.
CREATE STREAM INPUT (K DOUBLE KEY, ID INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE 'not a boolean' EMIT CHANGES;
--@test: comparison-expression - invalid where predicate - double expression
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Type error in WHERE expression: Should evaluate to boolean but is (RANDOM() + 1.5) (DOUBLE) instead.
CREATE STREAM INPUT (K DOUBLE KEY, ID INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE RANDOM() + 1.5 EMIT CHANGES;
--@test: comparison-expression - invalid where predicate - illegal expression
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Error in WHERE expression: Error processing expression: (true + 1.5). Unsupported arithmetic types. BOOLEAN DECIMAL
CREATE STREAM INPUT (K DOUBLE KEY, ID INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE TRUE + 1.5 EMIT CHANGES;
--@test: comparison-expression - invalid having predicate - type mismatch
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Error in HAVING expression: Cannot compare SUM(VALUE) (BIGINT) to '100' (STRING) with GREATER_THAN.
CREATE STREAM TEST (K BIGINT KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE T1 as select id, sum(value) as sum from test WINDOW TUMBLING (SIZE 30 SECONDS) group by id HAVING sum(value) > '100';
--@test: comparison-expression - invalid having predicate - type mismatch complex expression
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Error in HAVING expression: Cannot compare SUM(VALUE) (BIGINT) to '100' (STRING) with GREATER_THAN.
CREATE STREAM TEST (K BIGINT KEY, ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE TABLE T1 as select id, sum(value) as sum from test WINDOW TUMBLING (SIZE 30 SECONDS) group by id HAVING count(*) > 1 AND sum(value) > '100';
--@test: comparison-expression - invalid having predicate - type mismatch join
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Error in HAVING expression: Cannot compare SUM(I1_BAR) (INTEGER) to '100' (STRING) with GREATER_THAN.
CREATE STREAM INPUT_1 (K INT KEY, foo INT, bar INT) WITH (kafka_topic='t1', value_format='JSON');
CREATE STREAM INPUT_2 (K INT KEY, foo INT, bar INT) WITH (kafka_topic='t2', value_format='JSON');
CREATE TABLE OUTPUT AS SELECT I1.FOO, SUM(I1.BAR), SUM(I2.BAR) FROM INPUT_1 I1 JOIN INPUT_2 I2 WITHIN 1 MINUTE ON I1.FOO = I2.FOO group by I1.FOO HAVING SUM(I1.BAR) > '100' and SUM(I2.BAR) > 20;
